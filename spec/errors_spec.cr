require "./spec_helper"
require "file_utils"
include Croupier

describe "error taxonomy" do
  it "derives every deliberate raise from Croupier::Error" do
    # Computed at compile time: classes that fail to derive from
    # Croupier::Error get named here, and this should stay empty
    offenders = [] of String
    {% for klass in [Croupier::TaskDefinitionError, Croupier::CycleError, Croupier::UnknownTaskError,
                     Croupier::UnknownInputsError, Croupier::TaskVerificationError, Croupier::TaskFailure,
                     Croupier::RunFailure, Croupier::UnreachableTaskError, Croupier::UsageError] %}
      {% unless klass.ancestors.includes?(Croupier::Error) %} offenders << {{ klass.stringify }} {% end %}
    {% end %}
    offenders.should be_empty
  end

  it "raises typed errors for definition mistakes" do
    with_scenario("empty") do
      expect_raises(Croupier::TaskDefinitionError, "empty kv:// key") do
        Task.new(inputs: ["kv://"], id: "x", proc: TaskProc.new { "" })
      end
      expect_raises(Croupier::CycleError, /Cycle detected.*both an input and an output/) do
        Task.new(output: "o", inputs: ["o"], proc: TaskProc.new { "" })
      end
    end
  end

  it "raises typed errors for unknown names and graph cycles" do
    with_scenario("empty") do
      Task.new(output: "out", proc: TaskProc.new { "x" })
      expect_raises(Croupier::UnknownTaskError, "Unknown task") do
        TaskManager.add_input("nope", "x")
      end
      expect_raises(Croupier::CycleError, /Cycle detected.*is a key of task/) do
        TaskManager.add_input("out", "out")
      end
    end
  end

  it "lets callers rescue everything with one type" do
    with_scenario("empty") do
      caught = nil
      begin
        Task.new(inputs: ["kv://"], id: "x", proc: TaskProc.new { "" })
      rescue ex : Croupier::Error
        caught = ex
      end
      caught.should be_a(Croupier::TaskDefinitionError)
    end
  end
end
