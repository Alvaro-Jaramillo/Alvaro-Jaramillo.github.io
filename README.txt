How to apply this fix

1) Download and unzip this package.
2) Copy the file into your repo at:
   .github/workflows/rss-update.yml

3) Commit & push to main.

This prevents overlapping workflow runs and fixes the "main -> main (fetch first)" push error by rebasing before pushing.
