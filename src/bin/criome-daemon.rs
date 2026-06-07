use criome::command::CriomeDaemonCommand;

fn main() -> criome::Result<()> {
    CriomeDaemonCommand::from_environment().run()
}
