# vhs tapes/durability.tape  -> gifs/durability.gif
Output gifs/durability.gif
Set Shell bash
Set FontSize 16
Set Width 1150
Set Height 320
Set Padding 14

Type "# Crash a consumer mid-stream: Kafka replays from its offset; a queue loses events"
Enter
Sleep 1s
Type ".venv/bin/python src/demo.py durability --events 800"
Enter
Sleep 14s
