# SBM

This is a project for a PowerShell cmdlet that purges messages from a Service Bus Topic.

**Show your interest for this project by support giving a [thumbs up 👍](https://github.com/BernieWhite/SBM/discussions/1).**

## Basic usage

```powershell
Import-Module ./out/modules/SBM/SBM.dll;

Purge-Messages -ConnectionString "" -TopicName "" -SubscriptionName "" -Queue Standard, DeadLetter;
```
