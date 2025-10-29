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
      assert ShardInfo.test_partition_token([ShardInfo.serialize_component(:text, key)]) == value
    end)
    # the following cases use token generation produced by cqlsh, for example:
    # cqlsh:sleeper_test> select token(line_id) from  parlay_line where line_id=7386436132013076480;
    # system.token(line_id)
    # -----------------------
    #  -1446046784528965222
    assert ShardInfo.test_partition_token([ShardInfo.serialize_component(:bigint, 7386436132013076480)]) == -1446046784528965222

    channel_id = ShardInfo.serialize_component(:bigint, 7386833339577004032)
    bucket_id = ShardInfo.serialize_component(:int, 1245)
    assert ShardInfo.test_partition_token([channel_id, bucket_id]) == -3385343466860924788

    # partition_key [:sport, :season_type, :season, :user_id]
    # cluster_columns [:league_id]
    sport = ShardInfo.serialize_component(:text, "nfl")
    season_type = ShardInfo.serialize_component(:text, "regular")
    season = ShardInfo.serialize_component(:text, "2024")
    user_id = ShardInfo.serialize_component(:bigint, 7386436132013076480)
    assert ShardInfo.test_partition_token([sport, season_type, season, user_id]) == -4258149831554983843

    sport = ShardInfo.serialize_component(:text, "nfl")
    shard=ShardInfo.serialize_component(:smallint, 100)
    bucket=ShardInfo.serialize_component(:int, 200)
    bool_flag=ShardInfo.serialize_component(:boolean, true)

    assert ShardInfo.test_partition_token([sport, shard, bucket, bool_flag]) == 1614259030012692814
    bool_flag=ShardInfo.serialize_component(:boolean, false)
    assert ShardInfo.test_partition_token([sport, shard, bucket, bool_flag]) == 7282384892949881566
  end
end
