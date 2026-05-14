param(
    [string]$Message,
    [switch]$SkipPull,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )

    $display = "git " + ($Args -join " ")
    Write-Host ">> $display"

    if ($DryRun) {
        return
    }

    & git @Args

    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $display"
    }
}

function Get-GitOutput {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )

    $output = & git @Args 2>$null

    if ($LASTEXITCODE -ne 0) {
        throw "Git command failed: git $($Args -join ' ')"
    }

    return ($output | Out-String).Trim()
}

try {
    $null = Get-GitOutput -Args @("rev-parse", "--is-inside-work-tree")
} catch {
    throw "This script must be run from inside a Git repository."
}

$branch = Get-GitOutput -Args @("branch", "--show-current")

if ([string]::IsNullOrWhiteSpace($branch)) {
    throw "Unable to determine the current branch."
}

if ([string]::IsNullOrWhiteSpace($Message)) {
    $Message = "Backup $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
}

Write-Host "Branch: $branch"

if (-not $SkipPull) {
    Invoke-Git -Args @("pull", "--rebase", "origin", $branch)
}

$statusBefore = Get-GitOutput -Args @("status", "--short")

if ([string]::IsNullOrWhiteSpace($statusBefore)) {
    Write-Host "No modified files detected. Pushing current branch in case local commits are pending."
} else {
    Invoke-Git -Args @("add", "-A")

    if ($DryRun) {
        Write-Host "Dry run: would create commit with message '$Message'."
    } else {
        & git diff --cached --quiet
        $hasStagedChanges = ($LASTEXITCODE -ne 0)

        if ($hasStagedChanges) {
            Invoke-Git -Args @("commit", "-m", $Message)
        } else {
            Write-Host "No staged changes to commit after git add -A."
        }
    }
}

Invoke-Git -Args @("push", "origin", $branch)
Invoke-Git -Args @("status", "--short", "--branch")
