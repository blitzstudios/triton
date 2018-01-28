# Triton

Pure Elixir Cassandra ORM built on top of Xandra.

[Blog Post](https://blog.sleeperbot.com/triton-a-cassandra-orm-for-elixir-882dd8f11383)

## Configure Triton

Single Cluster

```elixir
config :triton,
  clusters: [
    [
      conn: Triton.Conn,
      nodes: ["127.0.0.1"],
      pool: Xandra.Cluster,
      underlying_pool: DBConnection.Poolboy,
      pool_size: 10,
      keyspace: "my_keyspace"
    ]
  ]
```

Multi-Cluster

```elixir
config :triton,
  clusters: [
    [
      conn: Cluster1.Conn,
      nodes: ["127.0.0.1"],
      pool: Xandra.Cluster,
      underlying_pool: DBConnection.Poolboy,
      pool_size: 10,
      keyspace: "cluster_1_keyspace"
    ],
    [
      conn: Cluster2.Conn,
      nodes: ["127.0.0.1"],
      pool: Xandra.Cluster,
      underlying_pool: DBConnection.Poolboy,
      pool_size: 10,
      keyspace: "cluster_2_keyspace"
    ]
  ]
```

## Defining a Keyspace

First, define your keyspace.  Triton will create the keyspace for your at compile time if it does not exist.

Currently Triton only supports a single Keyspace.

```elixir
defmodule Schema.Keyspace do
  use Triton.Keyspace

  keyspace :my_keyspace, conn: Triton.Conn do
    with_options [
      replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
    ]
  end
end
```

## Defining a Table

You can define as many tables as you want.  Triton will create tables for you if they do not exist.

If you would like Triton to auto-create tables for you at compile time, you must require your Keyspace module.

```elixir
defmodule Schema.User do
  require Schema.Keyspace
  use Triton.Table

  table :users, keyspace: Schema.Keyspace do
    field :user_id, :bigint, validators: [presence: true]  # validators using vex
    field :username, :text
    field :display_name, :text
    field :password, :text
    field :email, :text
    field :phone, :text
    field :notifications, {:map, "<text, text>"}
    field :friends, {:set, "<text>"}
    field :posts, {:list, "<text>"}
    partition_key [:user_id]
  end
end
```

## Defining a Materialized View

An example of a materialized view **users_by_email** with fields **user_id**, **email**, **display_name**, **password**.

Also demonstrates adding options like gc_grace_seconds and clustering_order_by.

```elixir
defmodule Schema.UserByEmail do
  require Schema.User  # if you want to auto-create at compile time
  use Triton.MaterializedView

  materialized_view :users_by_email, from: Schema.User do
    fields [
      :user_id,
      :email,
      :display_name,
      :password
    ]
    partition_key [:email]
    cluster_columns [:user_id]
    with_options [
      gc_grace_seconds: 172_800,
      clustering_order_by: [
        email: :asc,
        user_id: :desc
      ]
    ]
  end
end
```

An example of materialized view **users_by_email** with all fields

```elixir
defmodule Schema.UserByEmail do
  require Schema.User
  use Triton.MaterializedView

  materialized_view :users_by_email, from: Schema.User do
    fields :all
    partition_key [:email]
    cluster_columns [:user_id]
  end
end
```

## Querying

First, import Triton.Query

```elixir
alias Schema.User
import Triton.Query
```

Select a single user where user_id = <id> using a prepared statement.

```elixir
User
|> prepared(user_id: id)
|> select([:user_id, :username])
|> where(user_id: :user_id)
|> User.one
```

Select users with IDs of 1, 2, or 3

```elixir
User
|> select([:user_id, :username])
|> where(user_id: [in: [1, 2, 3]])
|> limit(10)
|> User.all
```

Select user with email **someone@gmail.com**

```elixir
UserByEmail
|> select([:display_name])
|> where(email: "someone@gmail.com")
|> User.one
```

## Inserting, Updating, & Deleting

Again, lets import Triton.Query for the necessary macros.

```elixir
alias Schema.User
import Triton.Query
```

Add a user (if it doesn't already exist) with username **username** using a prepared statement that substitutes **user_id** into **:user_id**

```elixir
User
|> prepared(user_id: user_id, username: username)
|> insert(user_id: :user_id, username: :username)
|> if_not_exists
|> User.save
```

Update a user's username, and make sure to check that their previous username was what we expected.

```elixir
User
|> update(username: username)
|> where(user_id: user_id)
|> constrain(username: previous_username)
|> User.save
```

Lets delete a user given a **user_id**

```elixir
User
|> prepared(user_id: user_id)
|> delete(:all)  # here :all refers to all fields
|> where(user_id: :user_id)
|> User.del
```

Lets delete that same user, with consistency: :quorum

```elixir
User
|> prepared(user_id: user_id)
|> delete(:all)  # here :all refers to all fields
|> where(user_id: :user_id)
|> User.del(consistency: :quorum)
```

Batch update 4 users in 1 Cassandra request.

```elixir
[
  User |> update(username: "username1") |> where(user_id: 1),
  User |> update(username: "username2") |> where(user_id: 2),
  User |> update(username: "username3") |> where(user_id: 3),
  User |> update(username: "username4") |> where(user_id: 4)
] |> User.batch_execute
```

## Working with Collections

Update the **notifications** map to {'mentions': '3', 'replies': '3'}.  Overwrites the entire map.

```elixir
User
|> update(notifications: "{'mentions': '5', 'replies': '3'}")
|> where(user_id: 10)
|> User.save
```

Update notification mentions to '5'.

```elixir
User
|> update("notifications['mentions']": "5")
|> where(user_id: 10)
|> User.save
```

Update the friends set

```elixir
User
|> update(friends: "{'jill', 'bob', 'emma'}")
|> where(user_id: 10)
|> User.save
```

Add a friend_id to friends set

```elixir
User
|> update(friends: "friends + {'oscar'}")
|> where(user_id: 10)
|> User.save
```

Remove friend from set

```elixir
User
|> update(friends: "friends - {'oscar'}")
|> where(user_id: 10)
|> User.save
```

Update the posts list

```elixir
User
|> update(posts: "['post1', 'post2', 'post3']")
|> where(user_id: 10)
|> User.save
```

Append to posts list

```elixir
User
|> update(posts: "posts + ['post4']")
|> where(user_id: 10)
|> User.save
```

Prepend to posts list

```elixir
User
|> update(posts: "['post0'] + posts")
|> where(user_id: 10)
|> User.save
```
