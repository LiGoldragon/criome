use criome::command::CriomeCommandLine;

fn main() -> criome::Result<()> {
    CriomeCommandLine::from_environment().run()
}
