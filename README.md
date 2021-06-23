# SBM

This is a project for a PowerShell cmdlet that purges messages from a Service Bus Topic.

**Show your interest for this project by support giving a [thumbs up 👍](https://github.com/BernieWhite/SBM/discussions/1).**

## Building

To build you'll need at least the .NET Core SDK 3.1 or greater.

Then:

- Clone the repo.
- Run `./build.ps1` from PowerShell within the repository root.

## Basic usage

```powershell
Import-Module ./out/modules/SBM/SBM.dll;

Purge-Messages -ConnectionString "" -TopicName "" -SubscriptionName "" -Queue Standard, DeadLetter;
```
