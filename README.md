# Walrus: Write-Ahead Log (Week 1)

A Write-Ahead Log (WAL) Library with a pluggable state machine interface. The WAL handles group commit, checksumming, rotation, compaction, and recovery. 

The reference implementation uses a simple Key-Value in-memory database.

Notes:

This should've been a simple project, but I became a victim of scope creep lol

For me, the hardest part of the project is the testing part. It's partly the reason why it became a pluggable interface to separate the WAL from the KV Operations. It also became harder to test once I moved from ASCII to bytes on the segment files, but I had to change it to work on the delimiter issue. It's funny how I had a eureka moment because of a leetcode question that I solved a while back ([string encode and decode](https://neetcode.io/problems/string-encode-and-decode/question?list=neetcode150)). I should've a crash test/harness test before I implemented anything, so that I won't waste too much time manually testing everything. 

The most interesting part for me is researching etcd, postgresql, and other database how they made use of WAL (the reason why I added more and more things). Out of these database/stores, the one that helped the most is etcd. It influenced how I implemented the checkpoint + snapshot, and also the rotate. It would've also been better to use protobuf for the logs, instead of, bytes. That would've helped a lot when I was figuring out and manually edited the byte locations of each header component lol. Also, I have the replication components, but I didn't really make use of it. Moving on to the week 2 now!



