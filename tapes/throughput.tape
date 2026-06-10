# vhs tapes/throughput.tape  -> gifs/throughput.gif
Output gifs/throughput.gif
Set Shell bash
Set FontSize 16
Set Width 1150
Set Height 380
Set Padding 14

Type "# Checkout rate as downstream work grows: inline (naive) vs Kafka"
Enter
Sleep 1s
Type ".venv/bin/python src/demo.py throughput --events 3000"
Enter
Sleep 24s
