defmodule Triton.APM.ShardInfo.Tests do
  use ExUnit.Case
  alias Triton.APM.ShardInfo

  # our primary concern is to make sure the partition token calculation has been ported properly
  test "should compute partition token correctly" do
    # these four cases are taken from the scylla rust driver test cases
    %{
      "test" => -6017608668500074083,
      "xd" => 4507812186440344727,
      "primary_key" => -1632642444691073360,
      "kremówki" => 4354931215268080151
    } |> Enum.each(fn {key, value} ->
      assert ShardInfo.test_partition_token([ShardInfo.serialize_component(key)]) == value
    end)
    # the following cases use token generation produced by cqlsh, for example:
    # cqlsh:sleeper_test> select token(line_id) from  parlay_line where line_id=7386436132013076480;
    # system.token(line_id)
    # -----------------------
    #  -1446046784528965222
    assert ShardInfo.test_partition_token([ShardInfo.serialize_component(7386436132013076480)]) == -1446046784528965222

    channel_id = 7386833339577004032 |> ShardInfo.serialize_component()
    bucket_id = 1245 |> ShardInfo.serialize_component()
    assert ShardInfo.test_partition_token([channel_id, bucket_id]) == -3385343466860924788
  end
end
