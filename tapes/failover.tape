# vhs tapes/failover.tape  -> gifs/failover.gif   (needs the 3-broker cluster up)
Output gifs/failover.gif
Set Shell bash
Set FontSize 16
Set Width 1150
Set Height 560
Set Padding 14

Type "# Kill a broker mid-stream: a replication-factor-3 topic keeps every event"
Enter
Sleep 1s
Type ".venv/bin/python src/demo.py failover --events 800"
Enter
Sleep 26s
