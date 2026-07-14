//! Bearer-JWT authentication and exact logical-target authorization.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use tokio::sync::RwLock;
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::domain::DatasetTarget;

/// Fixed maximum age of a successfully fetched JWKS document.
const JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// Fixed minimum interval between request-driven JWKS refreshes for unknown key ids.
const FORCED_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);

/// Fixed bound on unknown key ids remembered during the cooldown.
const MAX_NEGATIVE_KEY_IDS: usize = 1024;

/// Fixed maximum accepted JWKS response size.
const MAX_JWKS_BYTES: u64 = 1024 * 1024;

/// Closed authorization level understood by the search process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredRole {
    /// Access to the three public search methods.
    Search,
    /// Access to internal administrative methods.
    Admin,
}

/// Authentication and exact-target authorization contract used by gRPC handlers.
#[async_trait::async_trait]
pub trait RequestAuthorizer: Send + Sync + 'static {
    /// Verifies request credentials, target claims, and the required role.
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        target: &DatasetTarget,
        required_role: RequiredRole,
    ) -> Result<(), Status>;
}

/// Deployment-backed JWT validator using an HTTPS JWKS document.
pub struct JwtAuthorizer {
    issuer: String,
    audience: String,
    jwks_uri: String,
    client: reqwest::Client,
    keys: RwLock<CachedJwks>,
}

/// One validated JWKS fetch and its monotonic fetch instant.
struct CachedJwks {
    set: Arc<JwkSet>,
    fetched_at: Instant,
    last_forced_refresh: Option<Instant>,
    negative_key_ids: HashMap<String, Instant>,
}

/// Exact authorization claims carried by a search token.
#[derive(Debug, Deserialize)]
struct TargetClaims {
    org_id: Option<String>,
    tenant_id: Option<String>,
    namespace: Option<String>,
    scope: Option<String>,
    #[serde(default)]
    roles: Vec<String>,
}

impl JwtAuthorizer {
    /// Fetches the initial JWKS and constructs a fail-closed validator.
    pub async fn connect(issuer: String, audience: String, jwks_uri: String) -> Result<Self, String> {
        if issuer.is_empty() || audience.is_empty() {
            return Err("JWT issuer and audience must be non-empty".to_owned());
        }
        if !jwks_uri.starts_with("https://") {
            return Err("SEARCH_API_JWKS_URI must use https".to_owned());
        }
        let client = reqwest::Client::builder()
            .https_only(true)
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| format!("failed to configure JWKS client: {error}"))?;
        let set = fetch_jwks(&client, &jwks_uri).await?;
        Ok(Self {
            issuer,
            audience,
            jwks_uri,
            client,
            keys: RwLock::new(CachedJwks {
                set: Arc::new(set),
                fetched_at: Instant::now(),
                last_forced_refresh: None,
                negative_key_ids: HashMap::new(),
            }),
        })
    }

    /// Verifies that a non-expired JWKS is available, refreshing it when necessary.
    pub async fn health(&self) -> Result<(), String> {
        self.current_keys()
            .await
            .map(|_| ())
            .map_err(|status| status.message().to_owned())
    }

    /// Refreshes an expired JWKS document and returns the currently trusted set.
    async fn current_keys(&self) -> Result<Arc<JwkSet>, Status> {
        {
            let cached = self.keys.read().await;
            if cached.fetched_at.elapsed() < JWKS_REFRESH_INTERVAL {
                return Ok(cached.set.clone());
            }
        }
        self.refresh_keys().await
    }

    /// Serializes JWKS refreshes and fails closed when the identity provider is unavailable.
    async fn refresh_keys(&self) -> Result<Arc<JwkSet>, Status> {
        let mut cached = self.keys.write().await;
        if cached.fetched_at.elapsed() < JWKS_REFRESH_INTERVAL {
            return Ok(cached.set.clone());
        }
        let set = fetch_jwks(&self.client, &self.jwks_uri)
            .await
            .map_err(|_| Status::unavailable("identity provider unavailable"))?;
        cached.set = Arc::new(set);
        cached.fetched_at = Instant::now();
        Ok(cached.set.clone())
    }

    /// Resolves a signing key, refreshing once when a rotated key id is not cached.
    async fn decoding_key(&self, key_id: &str) -> Result<DecodingKey, Status> {
        let current = self.current_keys().await?;
        if let Some(jwk) = current.find(key_id) {
            return DecodingKey::from_jwk(jwk).map_err(|_| Status::unauthenticated("invalid bearer token"));
        }
        let mut cached = self.keys.write().await;
        if !should_force_refresh(&mut cached, key_id, Instant::now()) {
            return Err(Status::unauthenticated("invalid bearer token"));
        }
        let set = fetch_jwks(&self.client, &self.jwks_uri)
            .await
            .map_err(|_| Status::unavailable("identity provider unavailable"))?;
        cached.set = Arc::new(set);
        cached.fetched_at = Instant::now();
        if cached.set.find(key_id).is_none() {
            remember_negative_key(&mut cached, key_id, Instant::now());
            return Err(Status::unauthenticated("invalid bearer token"));
        }
        let jwk = cached
            .set
            .find(key_id)
            .ok_or_else(|| Status::unauthenticated("invalid bearer token"))?;
        DecodingKey::from_jwk(jwk).map_err(|_| Status::unauthenticated("invalid bearer token"))
    }

    /// Validates the token signature, registered claims, exact target, and required role.
    async fn authorize_token(
        &self,
        token: &str,
        target: &DatasetTarget,
        required_role: RequiredRole,
    ) -> Result<(), Status> {
        let header = decode_header(token).map_err(|_| Status::unauthenticated("invalid bearer token"))?;
        if header.alg != Algorithm::RS256 {
            return Err(Status::unauthenticated("invalid bearer token"));
        }
        let key_id = header
            .kid
            .as_deref()
            .ok_or_else(|| Status::unauthenticated("invalid bearer token"))?;
        let key = self.decoding_key(key_id).await?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        let claims = decode::<TargetClaims>(token, &key, &validation)
            .map_err(|_| Status::unauthenticated("invalid bearer token"))?
            .claims;
        authorize_claims(&claims, target, required_role)
    }
}

/// Applies role-specific authorization after cryptographic and registered-claim validation.
fn authorize_claims(claims: &TargetClaims, target: &DatasetTarget, required_role: RequiredRole) -> Result<(), Status> {
    match required_role {
        RequiredRole::Search => {
            if claims.org_id.as_deref() != Some(target.org_id.as_str())
                || claims.tenant_id.as_deref() != Some(target.tenant_id.as_str())
                || claims.namespace.as_deref() != Some(target.namespace.as_str())
                || !claims.roles.iter().any(|role| role == "search")
            {
                return Err(Status::permission_denied(
                    "token is not authorized for the requested target",
                ));
            }
        }
        RequiredRole::Admin => {
            if claims.scope.as_deref() != Some("lance-etl:prewarm") || !claims.roles.iter().any(|role| role == "admin")
            {
                return Err(Status::permission_denied("token does not grant prewarm administration"));
            }
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl RequestAuthorizer for JwtAuthorizer {
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        target: &DatasetTarget,
        required_role: RequiredRole,
    ) -> Result<(), Status> {
        let value = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("bearer token required"))?;
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
            .ok_or_else(|| Status::unauthenticated("bearer token required"))?;
        self.authorize_token(token, target, required_role).await
    }
}

/// Downloads and parses one bounded JWKS response.
async fn fetch_jwks(client: &reqwest::Client, uri: &str) -> Result<JwkSet, String> {
    let response = client
        .get(uri)
        .send()
        .await
        .map_err(|error| format!("failed to fetch JWKS: {error}"))?
        .error_for_status()
        .map_err(|error| format!("JWKS endpoint rejected the request: {error}"))?;
    if response.content_length().is_some_and(|length| length > MAX_JWKS_BYTES) {
        return Err("JWKS response exceeds the fixed byte limit".to_owned());
    }
    let body = response
        .bytes()
        .await
        .map_err(|error| format!("failed to read JWKS response: {error}"))?;
    if body.len() as u64 > MAX_JWKS_BYTES {
        return Err("JWKS response exceeds the fixed byte limit".to_owned());
    }
    serde_json::from_slice(&body).map_err(|error| format!("invalid JWKS response: {error}"))
}

/// Decides whether one unknown key id may trigger a network refresh in this cooldown window.
fn should_force_refresh(cached: &mut CachedJwks, key_id: &str, now: Instant) -> bool {
    cached
        .negative_key_ids
        .retain(|_, rejected_at| now.duration_since(*rejected_at) < FORCED_REFRESH_COOLDOWN);
    if cached.negative_key_ids.contains_key(key_id) {
        return false;
    }
    if cached
        .last_forced_refresh
        .is_some_and(|refreshed_at| now.duration_since(refreshed_at) < FORCED_REFRESH_COOLDOWN)
    {
        remember_negative_key(cached, key_id, now);
        return false;
    }
    cached.last_forced_refresh = Some(now);
    true
}

/// Remembers an unknown key id without allowing attacker-controlled unbounded growth.
fn remember_negative_key(cached: &mut CachedJwks, key_id: &str, now: Instant) {
    if cached.negative_key_ids.len() >= MAX_NEGATIVE_KEY_IDS {
        cached.negative_key_ids.clear();
    }
    cached.negative_key_ids.insert(key_id.to_owned(), now);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use jsonwebtoken::jwk::JwkSet;

    use super::{
        CachedJwks, FORCED_REFRESH_COOLDOWN, RequiredRole, TargetClaims, authorize_claims, should_force_refresh,
    };
    use crate::domain::DatasetTarget;

    /// Builds an empty cached set for exercising forced-refresh admission.
    fn empty_cache(now: Instant) -> CachedJwks {
        CachedJwks {
            set: Arc::new(JwkSet { keys: Vec::new() }),
            fetched_at: now,
            last_forced_refresh: None,
            negative_key_ids: HashMap::new(),
        }
    }

    #[test]
    fn repeated_unknown_key_ids_force_at_most_one_refresh_per_cooldown() {
        let now = Instant::now();
        let mut cached = empty_cache(now);
        assert!(should_force_refresh(&mut cached, "unknown-a", now));
        assert!(!should_force_refresh(
            &mut cached,
            "unknown-a",
            now + Duration::from_secs(1)
        ));
        assert!(!should_force_refresh(
            &mut cached,
            "unknown-b",
            now + Duration::from_secs(1)
        ));
        assert!(should_force_refresh(
            &mut cached,
            "unknown-b",
            now + FORCED_REFRESH_COOLDOWN + Duration::from_secs(1)
        ));
    }

    #[test]
    fn public_tokens_are_target_exact_while_scoped_admin_tokens_span_targets() {
        let target = DatasetTarget::new("org-a", "tenant-a", "namespace-a");
        let cross_target = TargetClaims {
            org_id: Some("org-b".to_owned()),
            tenant_id: Some("tenant-a".to_owned()),
            namespace: Some("namespace-a".to_owned()),
            scope: None,
            roles: vec!["search".to_owned()],
        };
        assert_eq!(
            authorize_claims(&cross_target, &target, RequiredRole::Search)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let admin = TargetClaims {
            org_id: None,
            tenant_id: None,
            namespace: None,
            scope: Some("lance-etl:prewarm".to_owned()),
            roles: vec!["admin".to_owned()],
        };
        assert!(authorize_claims(&admin, &target, RequiredRole::Admin).is_ok());
        assert_eq!(
            authorize_claims(&admin, &target, RequiredRole::Search)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}
