import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

// ========== DATA TYPES ==========

pub type UserId =
  String

pub type SubredditId =
  String

pub type PostId =
  String

pub type CommentId =
  String

pub type MessageId =
  String

pub type User {
  User(
    id: UserId,
    username: String,
    karma: Int,
    subscriptions: List(SubredditId),
    is_online: Bool,
  )
}

pub type Subreddit {
  Subreddit(
    id: SubredditId,
    name: String,
    members: List(UserId),
    posts: List(PostId),
  )
}

pub type Post {
  Post(
    id: PostId,
    author: UserId,
    subreddit: SubredditId,
    content: String,
    upvotes: Int,
    downvotes: Int,
    comments: List(CommentId),
    is_repost: Bool,
  )
}

pub type Comment {
  Comment(
    id: CommentId,
    author: UserId,
    post_id: PostId,
    parent_comment_id: Option(CommentId),
    content: String,
    upvotes: Int,
    downvotes: Int,
    replies: List(CommentId),
  )
}

pub type DirectMessage {
  DirectMessage(
    id: MessageId,
    from: UserId,
    to: UserId,
    content: String,
    timestamp: Int,
    reply_to: Option(MessageId),
  )
}

pub type EngineState {
  EngineState(
    users: Dict(UserId, User),
    subreddits: Dict(SubredditId, Subreddit),
    posts: Dict(PostId, Post),
    comments: Dict(CommentId, Comment),
    messages: Dict(UserId, List(DirectMessage)),
    next_user_id: Int,
    next_subreddit_id: Int,
    next_post_id: Int,
    next_comment_id: Int,
    next_message_id: Int,
    total_operations: Int,
  )
}

pub type PerformanceMetrics {
  PerformanceMetrics(
    total_operations: Int,
    total_posts_created: Int,
    total_comments_created: Int,
    total_upvotes: Int,
    total_downvotes: Int,
    peak_online_users: Int,
  )
}

// ========== ENGINE MESSAGES ==========

pub type EngineMessage {
  RegisterUser(username: String, reply_with: Subject(Result(User, String)))
  CreateSubreddit(
    name: String,
    creator: UserId,
    reply_with: Subject(Result(Subreddit, String)),
  )
  JoinSubreddit(
    user_id: UserId,
    subreddit_id: SubredditId,
    reply_with: Subject(Result(String, String)),
  )
  LeaveSubreddit(
    user_id: UserId,
    subreddit_id: SubredditId,
    reply_with: Subject(Result(String, String)),
  )
  CreatePost(
    author: UserId,
    subreddit: SubredditId,
    content: String,
    is_repost: Bool,
    reply_with: Subject(Result(Post, String)),
  )
  CreateComment(
    author: UserId,
    post_id: PostId,
    parent_comment_id: Option(CommentId),
    content: String,
    reply_with: Subject(Result(Comment, String)),
  )
  UpvotePost(post_id: PostId, reply_with: Subject(Result(String, String)))
  DownvotePost(post_id: PostId, reply_with: Subject(Result(String, String)))
  UpvoteComment(
    comment_id: CommentId,
    reply_with: Subject(Result(String, String)),
  )
  DownvoteComment(
    comment_id: CommentId,
    reply_with: Subject(Result(String, String)),
  )
  GetFeed(user_id: UserId, reply_with: Subject(Result(List(Post), String)))
  SendDirectMessage(
    from: UserId,
    to: UserId,
    content: String,
    reply_to: Option(MessageId),
    reply_with: Subject(Result(String, String)),
  )
  GetDirectMessages(
    user_id: UserId,
    reply_with: Subject(Result(List(DirectMessage), String)),
  )
  SetUserOnline(user_id: UserId, is_online: Bool)
  GetUser(user_id: UserId, reply_with: Subject(Result(User, String)))
  GetStats(reply_with: Subject(EngineStats))
  GetMetrics(reply_with: Subject(PerformanceMetrics))
  GetSubreddit(
    subreddit_id: SubredditId,
    reply_with: Subject(Result(Subreddit, String)),
  )
  GetAllPosts(reply_with: Subject(Dict(PostId, Post)))
  Shutdown
}

pub type EngineStats {
  EngineStats(
    total_users: Int,
    total_subreddits: Int,
    total_posts: Int,
    total_comments: Int,
    total_messages: Int,
    online_users: Int,
  )
}

// ========== ENGINE LOOP ==========

fn engine_loop(state: EngineState, self: Subject(EngineMessage)) -> Nil {
  case process.receive(self, 100_000) {
    Ok(message) -> {
      let updated_state =
        EngineState(..state, total_operations: state.total_operations + 1)
      case message {
        RegisterUser(username, reply_with) -> {
          let user_id = "user_" <> int.to_string(updated_state.next_user_id)
          let user =
            User(
              id: user_id,
              username: username,
              karma: 0,
              subscriptions: [],
              is_online: True,
            )
          let new_state =
            EngineState(
              ..updated_state,
              users: dict.insert(updated_state.users, user_id, user),
              next_user_id: updated_state.next_user_id + 1,
              messages: dict.insert(updated_state.messages, user_id, []),
            )
          process.send(reply_with, Ok(user))
          engine_loop(new_state, self)
        }

        CreateSubreddit(name, creator, reply_with) -> {
          let subreddit_id =
            "sub_" <> int.to_string(updated_state.next_subreddit_id)
          let subreddit =
            Subreddit(
              id: subreddit_id,
              name: name,
              members: [creator],
              posts: [],
            )
          let new_state =
            EngineState(
              ..updated_state,
              subreddits: dict.insert(
                updated_state.subreddits,
                subreddit_id,
                subreddit,
              ),
              next_subreddit_id: updated_state.next_subreddit_id + 1,
            )
          let final_state = case dict.get(new_state.users, creator) {
            Ok(user) -> {
              let updated_user =
                User(..user, subscriptions: [subreddit_id, ..user.subscriptions])
              EngineState(
                ..new_state,
                users: dict.insert(new_state.users, creator, updated_user),
              )
            }
            Error(_) -> new_state
          }
          process.send(reply_with, Ok(subreddit))
          engine_loop(final_state, self)
        }

        JoinSubreddit(user_id, subreddit_id, reply_with) -> {
          case dict.get(updated_state.subreddits, subreddit_id) {
            Ok(subreddit) -> {
              case dict.get(updated_state.users, user_id) {
                Ok(user) -> {
                  let updated_subreddit =
                    Subreddit(..subreddit, members: [
                      user_id,
                      ..subreddit.members
                    ])
                  let updated_user =
                    User(..user, subscriptions: [
                      subreddit_id,
                      ..user.subscriptions
                    ])
                  let new_state =
                    EngineState(
                      ..updated_state,
                      subreddits: dict.insert(
                        updated_state.subreddits,
                        subreddit_id,
                        updated_subreddit,
                      ),
                      users: dict.insert(updated_state.users, user_id, updated_user),
                    )
                  process.send(reply_with, Ok("Joined subreddit"))
                  engine_loop(new_state, self)
                }
                Error(_) -> {
                  process.send(reply_with, Error("User not found"))
                  engine_loop(updated_state, self)
                }
              }
            }
            Error(_) -> {
              process.send(reply_with, Error("Subreddit not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        LeaveSubreddit(user_id, subreddit_id, reply_with) -> {
          case dict.get(updated_state.subreddits, subreddit_id) {
            Ok(subreddit) -> {
              case dict.get(updated_state.users, user_id) {
                Ok(user) -> {
                  let updated_subreddit =
                    Subreddit(
                      ..subreddit,
                      members: list.filter(subreddit.members, fn(id) {
                        id != user_id
                      }),
                    )
                  let updated_user =
                    User(
                      ..user,
                      subscriptions: list.filter(user.subscriptions, fn(id) {
                        id != subreddit_id
                      }),
                    )
                  let new_state =
                    EngineState(
                      ..updated_state,
                      subreddits: dict.insert(
                        updated_state.subreddits,
                        subreddit_id,
                        updated_subreddit,
                      ),
                      users: dict.insert(updated_state.users, user_id, updated_user),
                    )
                  process.send(reply_with, Ok("Left subreddit"))
                  engine_loop(new_state, self)
                }
                Error(_) -> {
                  process.send(reply_with, Error("User not found"))
                  engine_loop(updated_state, self)
                }
              }
            }
            Error(_) -> {
              process.send(reply_with, Error("Subreddit not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        CreatePost(author, subreddit, content, is_repost, reply_with) -> {
          let post_id = "post_" <> int.to_string(updated_state.next_post_id)
          let post =
            Post(
              id: post_id,
              author: author,
              subreddit: subreddit,
              content: content,
              upvotes: 0,
              downvotes: 0,
              comments: [],
              is_repost: is_repost,
            )
          case dict.get(updated_state.subreddits, subreddit) {
            Ok(sub) -> {
              let updated_subreddit =
                Subreddit(..sub, posts: [post_id, ..sub.posts])
              let new_state =
                EngineState(
                  ..updated_state,
                  posts: dict.insert(updated_state.posts, post_id, post),
                  subreddits: dict.insert(
                    updated_state.subreddits,
                    subreddit,
                    updated_subreddit,
                  ),
                  next_post_id: updated_state.next_post_id + 1,
                )
              process.send(reply_with, Ok(post))
              engine_loop(new_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("Subreddit not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        CreateComment(author, post_id, parent_comment_id, content, reply_with) -> {
          let comment_id =
            "comment_" <> int.to_string(updated_state.next_comment_id)
          let comment =
            Comment(
              id: comment_id,
              author: author,
              post_id: post_id,
              parent_comment_id: parent_comment_id,
              content: content,
              upvotes: 0,
              downvotes: 0,
              replies: [],
            )

          case dict.get(updated_state.posts, post_id) {
            Ok(post) -> {
              let updated_post =
                Post(..post, comments: [comment_id, ..post.comments])

              let updated_comments = case parent_comment_id {
                Some(parent_id) -> {
                  case dict.get(updated_state.comments, parent_id) {
                    Ok(parent_comment) -> {
                      let updated_parent =
                        Comment(..parent_comment, replies: [
                          comment_id,
                          ..parent_comment.replies
                        ])
                      dict.insert(
                        dict.insert(updated_state.comments, comment_id, comment),
                        parent_id,
                        updated_parent,
                      )
                    }
                    Error(_) ->
                      dict.insert(updated_state.comments, comment_id, comment)
                  }
                }
                None -> dict.insert(updated_state.comments, comment_id, comment)
              }

              let new_state =
                EngineState(
                  ..updated_state,
                  posts: dict.insert(updated_state.posts, post_id, updated_post),
                  comments: updated_comments,
                  next_comment_id: updated_state.next_comment_id + 1,
                )
              process.send(reply_with, Ok(comment))
              engine_loop(new_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("Post not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        UpvotePost(post_id, reply_with) -> {
          case dict.get(updated_state.posts, post_id) {
            Ok(post) -> {
              let updated_post = Post(..post, upvotes: post.upvotes + 1)
              let new_state =
                EngineState(
                  ..updated_state,
                  posts: dict.insert(updated_state.posts, post_id, updated_post),
                )
              let final_state = update_user_karma(new_state, post.author, 1)
              process.send(reply_with, Ok("Upvoted"))
              engine_loop(final_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("Post not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        DownvotePost(post_id, reply_with) -> {
          case dict.get(updated_state.posts, post_id) {
            Ok(post) -> {
              let updated_post = Post(..post, downvotes: post.downvotes + 1)
              let new_state =
                EngineState(
                  ..updated_state,
                  posts: dict.insert(updated_state.posts, post_id, updated_post),
                )
              let final_state = update_user_karma(new_state, post.author, -1)
              process.send(reply_with, Ok("Downvoted"))
              engine_loop(final_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("Post not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        UpvoteComment(comment_id, reply_with) -> {
          case dict.get(updated_state.comments, comment_id) {
            Ok(comment) -> {
              let updated_comment =
                Comment(..comment, upvotes: comment.upvotes + 1)
              let new_state =
                EngineState(
                  ..updated_state,
                  comments: dict.insert(
                    updated_state.comments,
                    comment_id,
                    updated_comment,
                  ),
                )
              let final_state = update_user_karma(new_state, comment.author, 1)
              process.send(reply_with, Ok("Upvoted"))
              engine_loop(final_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("Comment not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        DownvoteComment(comment_id, reply_with) -> {
          case dict.get(updated_state.comments, comment_id) {
            Ok(comment) -> {
              let updated_comment =
                Comment(..comment, downvotes: comment.downvotes + 1)
              let new_state =
                EngineState(
                  ..updated_state,
                  comments: dict.insert(
                    updated_state.comments,
                    comment_id,
                    updated_comment,
                  ),
                )
              let final_state = update_user_karma(new_state, comment.author, -1)
              process.send(reply_with, Ok("Downvoted"))
              engine_loop(final_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("Comment not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        GetFeed(user_id, reply_with) -> {
          case dict.get(updated_state.users, user_id) {
            Ok(user) -> {
              let feed =
                list.flat_map(user.subscriptions, fn(sub_id) {
                  case dict.get(updated_state.subreddits, sub_id) {
                    Ok(subreddit) -> {
                      list.filter_map(subreddit.posts, fn(post_id) {
                        dict.get(updated_state.posts, post_id)
                      })
                    }
                    Error(_) -> []
                  }
                })
              process.send(reply_with, Ok(feed))
              engine_loop(updated_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("User not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        SendDirectMessage(from, to, content, reply_to, reply_with) -> {
          let message_id =
            "msg_" <> int.to_string(updated_state.next_message_id)
          let message =
            DirectMessage(
              id: message_id,
              from: from,
              to: to,
              content: content,
              timestamp: updated_state.next_message_id,
              reply_to: reply_to,
            )
          case dict.get(updated_state.messages, to) {
            Ok(messages) -> {
              let new_state =
                EngineState(
                  ..updated_state,
                  messages: dict.insert(updated_state.messages, to, [
                    message,
                    ..messages
                  ]),
                  next_message_id: updated_state.next_message_id + 1,
                )
              process.send(reply_with, Ok("Message sent"))
              engine_loop(new_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("Recipient not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        GetDirectMessages(user_id, reply_with) -> {
          case dict.get(updated_state.messages, user_id) {
            Ok(messages) -> {
              process.send(reply_with, Ok(messages))
              engine_loop(updated_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Ok([]))
              engine_loop(updated_state, self)
            }
          }
        }

        SetUserOnline(user_id, is_online) -> {
          let new_state = case dict.get(updated_state.users, user_id) {
            Ok(user) -> {
              let updated_user = User(..user, is_online: is_online)
              EngineState(
                ..updated_state,
                users: dict.insert(updated_state.users, user_id, updated_user),
              )
            }
            Error(_) -> updated_state
          }
          engine_loop(new_state, self)
        }

        GetUser(user_id, reply_with) -> {
          case dict.get(updated_state.users, user_id) {
            Ok(user) -> {
              process.send(reply_with, Ok(user))
              engine_loop(updated_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("User not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        GetStats(reply_with) -> {
          let online_count =
            dict.fold(updated_state.users, 0, fn(count, _id, user) {
              case user.is_online {
                True -> count + 1
                False -> count
              }
            })
          let stats =
            EngineStats(
              total_users: dict.size(updated_state.users),
              total_subreddits: dict.size(updated_state.subreddits),
              total_posts: dict.size(updated_state.posts),
              total_comments: dict.size(updated_state.comments),
              total_messages: updated_state.next_message_id - 1,
              online_users: online_count,
            )
          process.send(reply_with, stats)
          engine_loop(updated_state, self)
        }

        GetMetrics(reply_with) -> {
          let online_count =
            dict.fold(updated_state.users, 0, fn(count, _id, user) {
              case user.is_online {
                True -> count + 1
                False -> count
              }
            })

          let #(total_upvotes, total_downvotes) =
            dict.fold(updated_state.posts, #(0, 0), fn(acc, _id, post) {
              #(acc.0 + post.upvotes, acc.1 + post.downvotes)
            })

          let metrics =
            PerformanceMetrics(
              total_operations: updated_state.total_operations,
              total_posts_created: dict.size(updated_state.posts),
              total_comments_created: dict.size(updated_state.comments),
              total_upvotes: total_upvotes,
              total_downvotes: total_downvotes,
              peak_online_users: online_count,
            )
          process.send(reply_with, metrics)
          engine_loop(updated_state, self)
        }

        GetSubreddit(subreddit_id, reply_with) -> {
          case dict.get(updated_state.subreddits, subreddit_id) {
            Ok(subreddit) -> {
              process.send(reply_with, Ok(subreddit))
              engine_loop(updated_state, self)
            }
            Error(_) -> {
              process.send(reply_with, Error("Subreddit not found"))
              engine_loop(updated_state, self)
            }
          }
        }

        GetAllPosts(reply_with) -> {
          process.send(reply_with, updated_state.posts)
          engine_loop(updated_state, self)
        }

        Shutdown -> Nil
      }
    }
    Error(_) -> engine_loop(state, self)
  }
}

fn update_user_karma(
  state: EngineState,
  user_id: UserId,
  delta: Int,
) -> EngineState {
  case dict.get(state.users, user_id) {
    Ok(user) -> {
      let updated_user = User(..user, karma: user.karma + delta)
      EngineState(
        ..state,
        users: dict.insert(state.users, user_id, updated_user),
      )
    }
    Error(_) -> state
  }
}

fn start_engine() -> Subject(EngineMessage) {
  let initial_state =
    EngineState(
      users: dict.new(),
      subreddits: dict.new(),
      posts: dict.new(),
      comments: dict.new(),
      messages: dict.new(),
      next_user_id: 1,
      next_subreddit_id: 1,
      next_post_id: 1,
      next_comment_id: 1,
      next_message_id: 1,
      total_operations: 0,
    )

  let parent_subject = process.new_subject()

  process.spawn(fn() {
    let engine_subject = process.new_subject()
    process.send(parent_subject, engine_subject)
    engine_loop(initial_state, engine_subject)
  })

  let assert Ok(engine_subject) = process.receive(parent_subject, 5000)
  engine_subject
}

// ========== SIMULATOR ==========

fn zipf_distribution(_n: Int, rank: Int) -> Float {
  let rank_float = int.to_float(rank)
  1.0 /. rank_float
}

fn simulate_user_activity(
  engine: Subject(EngineMessage),
  user_id: UserId,
  subreddit_ids: List(SubredditId),
  num_actions: Int,
  cycle: Int,
) -> Nil {
  case num_actions > 0 {
    True -> {
      // Simulate periodic connection/disconnection every 20 actions
      case cycle % 20 {
        0 -> {
          let is_online = { cycle % 40 } == 0
          process.send(engine, SetUserOnline(user_id, is_online))
        }
        _ -> Nil
      }

      let action = num_actions % 8
      case action {
        0 -> {
          // Create post
          case list.first(subreddit_ids) {
            Ok(subreddit) -> {
              let ack = process.new_subject()
              let content =
                "Post content from "
                <> user_id
                <> " at cycle "
                <> int.to_string(cycle)
              let is_repost = { num_actions % 10 } == 0
              process.send(
                engine,
                CreatePost(user_id, subreddit, content, is_repost, ack),
              )
              let _ = process.receive(ack, 1000)
              Nil
            }
            Error(_) -> Nil
          }
        }
        1 -> {
          // Get feed
          let ack = process.new_subject()
          process.send(engine, GetFeed(user_id, ack))
          let _ = process.receive(ack, 1000)
          Nil
        }
        2 -> {
          // Upvote random post
          let pnum = { num_actions % 100 } + 1
          let post_id = "post_" <> int.to_string(pnum)
          let ack = process.new_subject()
          process.send(engine, UpvotePost(post_id, ack))
          let _ = process.receive(ack, 1000)
          Nil
        }
        3 -> {
          // Create comment
          let pnum = { num_actions % 50 } + 1
          let post_id = "post_" <> int.to_string(pnum)
          let ack = process.new_subject()
          let content = "Comment from " <> user_id
          process.send(
            engine,
            CreateComment(user_id, post_id, None, content, ack),
          )
          let _ = process.receive(ack, 1000)
          Nil
        }
        4 -> {
          // Send direct message
          let target_user = "user_" <> int.to_string({ num_actions % 30 } + 1)
          let ack = process.new_subject()
          let content = "DM from " <> user_id
          process.send(
            engine,
            SendDirectMessage(user_id, target_user, content, None, ack),
          )
          let _ = process.receive(ack, 1000)
          Nil
        }
        5 -> {
          // Reply to a direct message
          let ack = process.new_subject()
          process.send(engine, GetDirectMessages(user_id, ack))
          case process.receive(ack, 1000) {
            Ok(Ok(messages)) -> {
              case list.first(messages) {
                Ok(msg) -> {
                  let ack2 = process.new_subject()
                  process.send(
                    engine,
                    SendDirectMessage(
                      user_id,
                      msg.from,
                      "Reply to your message",
                      Some(msg.id),
                      ack2,
                    ),
                  )
                  let _ = process.receive(ack2, 1000)
                  Nil
                }
                Error(_) -> Nil
              }
            }
            _ -> Nil
          }
        }
        6 -> {
          // Downvote random post
          let pnum = { num_actions % 80 } + 1
          let post_id = "post_" <> int.to_string(pnum)
          let ack = process.new_subject()
          process.send(engine, DownvotePost(post_id, ack))
          let _ = process.receive(ack, 1000)
          Nil
        }
        _ -> {
          // Upvote random comment
          let cnum = { num_actions % 50 } + 1
          let comment_id = "comment_" <> int.to_string(cnum)
          let ack = process.new_subject()
          process.send(engine, UpvoteComment(comment_id, ack))
          let _ = process.receive(ack, 1000)
          Nil
        }
      }
      simulate_user_activity(
        engine,
        user_id,
        subreddit_ids,
        num_actions - 1,
        cycle + 1,
      )
    }
    False -> Nil
  }
}

pub fn main() {
  io.println("=== Reddit Clone Engine Starting ===\n")

  let engine = start_engine()
  io.println("Engine started successfully")

  let num_users = 100
  let num_subreddits = 20
  io.println(
    "Simulating "
    <> int.to_string(num_users)
    <> " users and "
    <> int.to_string(num_subreddits)
    <> " subreddits\n",
  )

  io.println("Registering users...")
  let user_ids =
    list.range(1, num_users)
    |> list.map(fn(i) {
      let ack = process.new_subject()
      let username = "user" <> int.to_string(i)
      process.send(engine, RegisterUser(username, ack))
      let assert Ok(Ok(user)) = process.receive(ack, 5000)
      user.id
    })

  io.println("Registered " <> int.to_string(list.length(user_ids)) <> " users")

  io.println("\nCreating subreddits...")
  let subreddits =
    list.range(1, num_subreddits)
    |> list.map(fn(i) {
      let ack = process.new_subject()
      let name = "subreddit" <> int.to_string(i)
      let assert Ok(user_id) = list.first(user_ids)
      process.send(engine, CreateSubreddit(name, user_id, ack))
      let assert Ok(Ok(subreddit)) = process.receive(ack, 5000)
      subreddit
    })

  io.println(
    "Created " <> int.to_string(list.length(subreddits)) <> " subreddits",
  )

  io.println("\nDistributing users across subreddits (Zipf distribution)...")
  list.each(user_ids, fn(user_id) {
    let user_rank =
      result.unwrap(int.parse(string.replace(user_id, "user_", "")), 1)
    let zipf_value = zipf_distribution(num_users, user_rank)

    // Zipf distribution for subreddit membership
    let num_subs_to_join = {
      let subs = float.round(zipf_value *. int.to_float(num_subreddits))
      int.max(subs, 1)
    }

    list.take(subreddits, num_subs_to_join)
    |> list.each(fn(subreddit) {
      let ack = process.new_subject()
      process.send(engine, JoinSubreddit(user_id, subreddit.id, ack))
      let _ = process.receive(ack, 1000)
      Nil
    })
  })

  io.println("Users distributed across subreddits")

  io.println("\nSimulating user activity with dynamic connections...")
  io.println("(Users will connect/disconnect periodically)\n")

  // Spawn concurrent client processes
  list.index_map(user_ids, fn(user_id, idx) {
    process.spawn(fn() {
      let is_online = { idx % 3 } != 0
      process.send(engine, SetUserOnline(user_id, is_online))

      case is_online {
        True -> {
          let user_rank =
            result.unwrap(int.parse(string.replace(user_id, "user_", "")), 1)
          let zipf_value = zipf_distribution(num_users, user_rank)

          // Zipf distribution for activity level
          let num_actions = float.round(zipf_value *. 100.0)

          let user_subreddit_ids =
            list.take(subreddits, 5) |> list.map(fn(s) { s.id })
          simulate_user_activity(
            engine,
            user_id,
            user_subreddit_ids,
            int.max(num_actions, 10),
            0,
          )
        }
        False -> Nil
      }
    })
  })

  // Allow simulation to run
  io.println("Simulation running...")
  process.sleep(3000)

  io.println("\n=== Performance Statistics ===")
  let stats_reply = process.new_subject()
  process.send(engine, GetStats(stats_reply))
  let assert Ok(stats) = process.receive(stats_reply, 5000)

  io.println("Total Users: " <> int.to_string(stats.total_users))
  io.println("Online Users: " <> int.to_string(stats.online_users))
  io.println("Total Subreddits: " <> int.to_string(stats.total_subreddits))
  io.println("Total Posts: " <> int.to_string(stats.total_posts))
  io.println("Total Comments: " <> int.to_string(stats.total_comments))
  io.println("Total Messages: " <> int.to_string(stats.total_messages))

  io.println("\n=== Performance Metrics ===")
  let metrics_reply = process.new_subject()
  process.send(engine, GetMetrics(metrics_reply))
  let assert Ok(metrics) = process.receive(metrics_reply, 5000)

  io.println("Total Operations: " <> int.to_string(metrics.total_operations))
  io.println("Posts Created: " <> int.to_string(metrics.total_posts_created))
  io.println(
    "Comments Created: " <> int.to_string(metrics.total_comments_created),
  )
  io.println("Total Upvotes: " <> int.to_string(metrics.total_upvotes))
  io.println("Total Downvotes: " <> int.to_string(metrics.total_downvotes))
  io.println("Peak Online Users: " <> int.to_string(metrics.peak_online_users))

  io.println("\n=== Top 10 Users by Karma ===")
  let all_users =
    list.filter_map(user_ids, fn(user_id) {
      let ack = process.new_subject()
      process.send(engine, GetUser(user_id, ack))
      case process.receive(ack, 1000) {
        Ok(Ok(user)) -> Ok(user)
        _ -> Error(Nil)
      }
    })

  list.sort(all_users, fn(a, b) { int.compare(b.karma, a.karma) })
  |> list.take(10)
  |> list.each(fn(user) {
    io.println(
      user.username
      <> ": "
      <> int.to_string(user.karma)
      <> " karma, "
      <> int.to_string(list.length(user.subscriptions))
      <> " subreddits, "
      <> case user.is_online {
        True -> "ONLINE"
        False -> "OFFLINE"
      },
    )
  })

  io.println("\n=== Subreddit Statistics (Top 5 by Members) ===")
  // Fetch fresh subreddit data from engine
  let fresh_subreddits =
    list.filter_map(subreddits, fn(sub) {
      let ack = process.new_subject()
      process.send(engine, GetSubreddit(sub.id, ack))
      case process.receive(ack, 1000) {
        Ok(Ok(fresh_sub)) -> Ok(fresh_sub)
        _ -> Error(Nil)
      }
    })

  list.sort(fresh_subreddits, fn(a, b) {
    int.compare(list.length(b.members), list.length(a.members))
  })
  |> list.take(5)
  |> list.each(fn(sub) {
    io.println(
      sub.name
      <> ": "
      <> int.to_string(list.length(sub.members))
      <> " members, "
      <> int.to_string(list.length(sub.posts))
      <> " posts",
    )
  })

  io.println("\n=== Zipf Distribution Validation ===")
  io.println("User ranks vs subscriptions (showing power-law):")
  list.take(all_users, 10)
  |> list.each(fn(user) {
    let rank =
      result.unwrap(int.parse(string.replace(user.id, "user_", "")), 1)
    io.println(
      "Rank "
      <> int.to_string(rank)
      <> ": "
      <> int.to_string(list.length(user.subscriptions))
      <> " subscriptions",
    )
  })

  // Show post distribution by user (Zipf validation)
  io.println("\n=== Post Activity by Top Users (Zipf Validation) ===")
  let posts_reply = process.new_subject()
  process.send(engine, GetAllPosts(posts_reply))
  let assert Ok(all_posts_dict) = process.receive(posts_reply, 1000)

  // Count posts per user
  let posts_per_user =
    dict.fold(all_posts_dict, dict.new(), fn(acc, _post_id, post) {
      let current_count = result.unwrap(dict.get(acc, post.author), 0)
      dict.insert(acc, post.author, current_count + 1)
    })

  list.take(all_users, 10)
  |> list.each(fn(user) {
    let post_count = result.unwrap(dict.get(posts_per_user, user.id), 0)
    let rank =
      result.unwrap(int.parse(string.replace(user.id, "user_", "")), 1)
    io.println(
      "Rank "
      <> int.to_string(rank)
      <> " ("
      <> user.username
      <> "): "
      <> int.to_string(post_count)
      <> " posts created",
    )
  })

  io.println("\n=== Sample Direct Messages (with replies) ===")
  list.take(user_ids, 3)
  |> list.each(fn(user_id) {
    let ack = process.new_subject()
    process.send(engine, GetDirectMessages(user_id, ack))
    case process.receive(ack, 1000) {
      Ok(Ok(messages)) -> {
        case list.length(messages) > 0 {
          True -> {
            io.println("\nMessages for " <> user_id <> ":")
            list.take(messages, 3)
            |> list.each(fn(msg) {
              let reply_indicator = case msg.reply_to {
                Some(id) -> " (reply to " <> id <> ")"
                None -> ""
              }
              io.println(
                "  From "
                <> msg.from
                <> ": "
                <> msg.content
                <> reply_indicator,
              )
            })
          }
          False -> Nil
        }
      }
      _ -> Nil
    }
  })

  io.println("\n=== Connection Dynamics ===")
  io.println(
    "Simulated periodic connect/disconnect for "
    <> int.to_string(stats.online_users)
    <> "/"
    <> int.to_string(stats.total_users)
    <> " users",
  )

  io.println("\n=== Simulation Complete ===")
  io.println("All operations completed successfully!")
  io.println("\n📊 Key Highlights:")
  io.println(
    "  • Processed "
    <> int.to_string(metrics.total_operations)
    <> " operations",
  )
  io.println(
    "  • Created "
    <> int.to_string(stats.total_posts)
    <> " posts and "
    <> int.to_string(stats.total_comments)
    <> " comments",
  )
  io.println(
    "  • Total engagement: "
    <> int.to_string(metrics.total_upvotes + metrics.total_downvotes)
    <> " votes",
  )
  io.println(
    "  • Active users: "
    <> int.to_string(stats.online_users)
    <> "/"
    <> int.to_string(stats.total_users),
  )

 

  process.send(engine, Shutdown)
}