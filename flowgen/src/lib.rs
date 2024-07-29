pub mod google {
    #[path = ""]
    pub mod storage {
        #[path = "google.storage.v2.rs"]
        pub mod v2;
        pub const ENDPOINT: &'static str = "https://storage.googleapis.com";
    }

    #[path = ""]
    pub mod iam {
        #[path = "google.iam.v1.rs"]
        pub mod v1;
    }

    #[path = "google.r#type.rs"]
    pub mod r#type;
}
pub mod salesforce {
    #[path = ""]
    pub mod eventbus {
        #[path = "eventbus.v1.rs"]
        pub mod v1;
        pub const GLOBAL_ENDPOINT: &str = "https://api.pubsub.salesforce.com";
        pub const DE_ENDPOINT: &str = "https://api.deu.pubsub.salesforce.com";
    }
    pub mod auth;
}
