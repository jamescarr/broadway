defmodule OffBroadway.Jetstream.AcknowledgerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias OffBroadway.Jetstream.Acknowledger

  describe "configure/3" do
    test "merges valid actions into the ack data" do
      assert Acknowledger.configure(:ack_ref, %{reply_to: "r"}, on_failure: {:nack, 10}) ==
               {:ok, %{reply_to: "r", on_failure: {:nack, 10}}}
    end

    test "rejects unknown options and invalid actions" do
      assert_raise ArgumentError, "unsupported configure_ack option :unknown", fn ->
        Acknowledger.configure(:ack_ref, %{reply_to: "r"}, unknown: :ack)
      end

      assert_raise ArgumentError, ~r/expected one of .* got: :bogus/, fn ->
        Acknowledger.configure(:ack_ref, %{reply_to: "r"}, on_success: :bogus)
      end
    end
  end

  test "ack/3 logs and returns :ok when the connection is unavailable" do
    ack_ref = {Acknowledger, :"ack_test_#{System.unique_integer([:positive])}"}

    Acknowledger.put_config(ack_ref, %{
      connection_name: :not_a_gnat_connection,
      on_success: :ack,
      on_failure: :nack
    })

    on_exit(fn -> :persistent_term.erase(ack_ref) end)

    message = %Broadway.Message{
      data: "data",
      acknowledger: {Acknowledger, ack_ref, %{reply_to: "$JS.ACK.S.C.1.1.1.0.0"}}
    }

    log =
      capture_log(fn ->
        assert Acknowledger.ack(ack_ref, [message], []) == :ok
      end)

    assert log =~ "failed to :ack message $JS.ACK.S.C.1.1.1.0.0"
  end
end
