# For tasks/generators testing
Mix.start()
Mix.shell(Mix.Shell.Process)
Logger.configure(level: :info)

Code.require_file("../support/file_helpers.exs", __DIR__)
ExUnit.start()
