//! Bounded global and per-tenant search admission.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

use crate::domain::DatasetTarget;

/// Fixed maximum number of inactive tenant semaphore entries retained between requests.
const MAX_INACTIVE_TENANT_ENTRIES: usize = 4096;

/// Immediate-shed admission controller that prevents one tenant from consuming global capacity.
pub struct AdmissionController {
    global: Arc<Semaphore>,
    per_tenant_limit: usize,
    tenants: Mutex<HashMap<DatasetTarget, Weak<Semaphore>>>,
}

/// Owned permits held for the complete lifetime of one admitted request.
#[derive(Debug)]
pub struct AdmissionPermit {
    permits: [OwnedSemaphorePermit; 2],
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let _ = self.permits.len();
    }
}

impl AdmissionController {
    /// Creates a controller with fixed process-global and per-tenant concurrency bounds.
    pub fn new(global_limit: usize, per_tenant_limit: usize) -> Result<Self, String> {
        if global_limit == 0 || per_tenant_limit == 0 || per_tenant_limit > global_limit {
            return Err("admission limits must satisfy 0 < per_tenant <= global".to_owned());
        }
        Ok(Self {
            global: Arc::new(Semaphore::new(global_limit)),
            per_tenant_limit,
            tenants: Mutex::new(HashMap::new()),
        })
    }

    /// Acquires both bounds without queueing, returning overload immediately when either is full.
    pub fn admit(&self, target: &DatasetTarget) -> Result<AdmissionPermit, Status> {
        let tenant = self.tenant_semaphore(target);
        let tenant_permit = tenant
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("tenant search capacity exhausted"))?;
        let global_permit = self
            .global
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("search capacity exhausted"))?;
        Ok(AdmissionPermit {
            permits: [tenant_permit, global_permit],
        })
    }

    /// Returns the stable semaphore for one logical tenant and prunes inactive entries boundedly.
    fn tenant_semaphore(&self, target: &DatasetTarget) -> Arc<Semaphore> {
        let mut tenants = self.tenants.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if tenants.len() >= MAX_INACTIVE_TENANT_ENTRIES {
            tenants.retain(|_, semaphore| semaphore.strong_count() > 0);
        }
        if let Some(semaphore) = tenants.get(target).and_then(Weak::upgrade) {
            return semaphore;
        }
        let semaphore = Arc::new(Semaphore::new(self.per_tenant_limit));
        tenants.insert(target.clone(), Arc::downgrade(&semaphore));
        semaphore
    }
}

#[cfg(test)]
mod tests {
    use super::AdmissionController;
    use crate::domain::DatasetTarget;

    /// Builds one validated logical target.
    fn target(tenant: &str) -> DatasetTarget {
        DatasetTarget {
            org_id: "org".to_owned(),
            tenant_id: tenant.to_owned(),
            namespace: "namespace".to_owned(),
        }
    }

    #[test]
    fn tenant_and_global_limits_shed_without_queueing() {
        let admission = AdmissionController::new(2, 1).unwrap();
        let first = admission.admit(&target("a")).unwrap();
        assert_eq!(
            admission.admit(&target("a")).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        let second = admission.admit(&target("b")).unwrap();
        assert_eq!(
            admission.admit(&target("c")).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        drop(first);
        assert!(admission.admit(&target("c")).is_ok());
        drop(second);
    }
}
