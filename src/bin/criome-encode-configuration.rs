use criome::deploy_encode::CriomeConfigurationEncoder;

fn main() {
    if let Err(error) = CriomeConfigurationEncoder::from_environment().run() {
        eprintln!("criome-encode-configuration: {error}");
        std::process::exit(1);
    }
}
