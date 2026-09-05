# Renders docs/autopilot-loop.gif: the autonomous loop end to end, on real
# provider data.
#   vhs docs/autopilot-loop.tape        (needs `make up`, autopilot workers stopped)
#
# Starts the REAL `make autopilot` (default stdout sink — nothing posts to
# Slack, no token needed) in the background, then fires `make demo-brief`,
# which emits the `opened` lifecycle event for a real already-indexed
# incident (freshet/autopilot/demo_trigger.py: "the brief is assembled from
# the provider's own updates by the same code path a live incident takes").
# stderr is dropped so only the clean stdout lines show: rdkafka connection
# noise and the bge model-load progress bar both go there. The autopilot
# debounces ~45s before composing, so the model load and the debounce wait
# are hidden — not the output.

Output docs/autopilot-loop.gif

Set Shell zsh
Set FontSize 13
Set Width 1320
Set Height 780
Set Padding 20
Set TypingSpeed 45ms
Set Theme "Catppuccin Mocha"

Type "PYTHONUNBUFFERED=1 make autopilot 2>/dev/null &"
Enter
Sleep 700ms

# hidden: bge model load
Hide
Sleep 20s
Show

# recorded: the trigger firing on a real indexed incident
Type "make demo-brief"
Sleep 700ms
Enter
Sleep 5s

# hidden: autopilot's debounce window + LLM composition
Hide
Sleep 60s
Show

# recorded: the cited brief landing
Sleep 8s
