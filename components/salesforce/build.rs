fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Salesforce PubSub.
    // https://github.com/forcedotcom/pub-sub-api
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src")
        .compile(&["proto/pubsub/pubsub_api.proto"], &["proto/pubsub"])?;
    Ok(())
}
