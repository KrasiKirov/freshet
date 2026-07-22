# Renders docs/autopilot-loop.gif: the autonomous loop end to end.
#   vhs docs/autopilot-loop.tape        (needs `make up` first)
#
# Runs the REAL `make slack-demo` in dry-run mode (posts nothing, no token
# needed). The pipe strips rdkafka connection warnings and the raw Block Kit
# dict dump, leaving the human-readable brief + postmortem. PYTHONUNBUFFERED
# keeps the background workers' output in chronological order through the pipe.
# The ~25s of model load and debounce wait is hidden, not the output.

Output docs/autopilot-loop.gif

Set Shell zsh
Set FontSize 13
Set Width 1320
Set Height 780
Set Padding 20
Set TypingSpeed 45ms
Set Theme "Catppuccin Mocha"

Type "PYTHONUNBUFFERED=1 make slack-demo 2>/dev/null | grep -v blocks="
Sleep 700ms
Enter

# recorded: the pipeline spinning up and the incident being injected
Sleep 5s

# hidden: bge model load + autopilot debounce + drain
Hide
Sleep 40s
Show

# recorded: the cited brief and threaded postmortem
Sleep 7s
