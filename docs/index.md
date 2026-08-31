---
layout: home

hero:
  name: maestro
  text: One soloist writes the score. Every player turns the page together.
  tagline: Atomic in-memory state replication for Go, from one writer to every replica. Every reader flips to the new version together, or keeps the old one — partial updates are never observable.
  image:
    src: /logo.png
    alt: maestro
  actions:
    - theme: brand
      text: Introduction
      link: /guide/introduction
    - theme: alt
      text: Getting Started
      link: /guide/getting-started
    - theme: alt
      text: GitHub
      link: https://github.com/foomo/maestro

features:
  - title: Three-phase commit
    details: CanCommit / PreCommit / DoCommit over NATS. A player that votes no, or fails to stage, aborts the round for everyone — no reader is left half-updated.
  - title: Pluggable BlobStore
    details: Control-plane traffic (rounds, votes, heartbeats) stays on NATS. File bytes move through a separate BlobStore — localfs ships in-box, swap in S3 or your own.
  - title: No leader election
    details: Soloist is a replicas=1 deployment with stable DNS. Restart is a brief disconnect; players reconnect and resync if they drifted.
---
