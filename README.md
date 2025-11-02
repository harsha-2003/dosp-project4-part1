
## **DOSP – Distributed Operating Systems Principles---Reddit clone**.


The objective of this project is to build a **distributed Reddit-style backend engine** using Gleam and Erlang processes, along with a **client simulator** that stresses the system with realistic workloads, Zipf-distributed subreddit popularity, repost behavior, and live user connection dynamics.
## 👥 Team Members

| Name | UFID | Role |
|------|------|------|
| Harshavardhan Reddy Jonnala | 1511-8670 | 
| Tanuja Naga Sai Palapati |  8947-5480 | 
## 🧠 Objective

Design and implement a backend system that supports:

- User registration
- Subreddit creation, join, and leave
- Posting text messages
- Hierarchical comments
- Upvote / downvote logic with karma computation
- Direct messages and threaded replies
- User feed generation (time-sorted)
- Client simulation with concurrency
- Zipf distribution for subreddit popularity and posting frequency
- Online / offline cycles to mimic real network behavior
- Performance metrics recording

This implementation prepares the system for **Part-2**, where REST APIs and Websockets will be added.

---

## ⚙️ Tech Stack

| Component | Technology |
|----------|-----------|
Language | **Gleam 0.33.x**
Runtime | **Erlang/OTP 26**
Concurrency Model | Actor Model (process mailboxes)
Data Storage | In-memory dictionaries (no DB)
Metrics | CSV + console summary
Simulation Load | Configurable (100–1000+ users)

---

## 🏗 System Architecture

### Components
- **Engine Process**
  - Stores all global Reddit state
  - Processes all client actions
  - Maintains users, subreddits, posts, comments, messages

- **Client Processes**
  - Simulate thousands of users
  - Perform actions such as posting, commenting, upvoting
  - Random online/offline cycles

### Actor Model Summary
- Asynchronous message passing
- No shared memory
- Deterministic engine mailbox processing
- High concurrency via Erlang VM

---

## ✅ Features Implemented

| Feature | Status |
|--------|--------|
Register user | ✅
Create / join / leave subreddit | ✅
Post text content | ✅
Hierarchical comments | ✅
Upvote / downvote + karma | ✅
DM + reply threading | ✅
User feed sorted by timestamp | ✅
Thousands of simulated users | ✅
Zipf subreddit & posting behavior | ✅
Periodic online/offline state | ✅
Performance metrics + CSV output | ✅

---
## commands to run
gleam build
gleam run
---

## output
<img width="974" height="825" alt="Screenshot 2025-11-02 155913" src="https://github.com/user-attachments/assets/8312479e-93b9-4d27-ab3e-9adb49edd0f9" />
<img width="711" height="967" alt="Screenshot 2025-11-02 155932" src="https://github.com/user-attachments/assets/90e53e9e-e92e-4f4f-8305-b7189038b221" />
<img width="772" height="357" alt="Screenshot 2025-11-02 155950" src="https://github.com/user-attachments/assets/e5a37de2-165e-4b7b-a6fe-84b0355e103d" />


