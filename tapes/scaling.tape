# vhs tapes/scaling.tape  -> gifs/scaling.gif
Output gifs/scaling.gif
Set Shell bash
Set FontSize 16
Set Width 1150
Set Height 360
Set Padding 14

Type "# One consumer group: throughput rises as you add consumers (up to #partitions)"
Enter
Sleep 1s
Type ".venv/bin/python src/demo.py scaling --events 2000 --partitions 8 --work-ms 2"
Enter
Sleep 30s
