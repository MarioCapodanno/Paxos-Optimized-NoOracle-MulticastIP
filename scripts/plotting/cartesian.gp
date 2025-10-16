datafile="logs/latency_client1"
output="logs/plot_cartesian.pdf"

# General plot appearence
set terminal pdfcairo size 8,5 enhanced color font "Times-Roman,30"
set output output

set title "Latency"
set xlabel "Sample"
set ylabel "Latency (us)"
set xtics font ",25"
set ytics font ",25"
set xtics rotate by 45 right
set grid xtics ytics

set key font ",25"
set key right top

# Plot
stats datafile using 1 nooutput
avg = STATS_mean

plot datafile with linespoints lw 2 lc "#112C80" title "Latency", avg lw 5 lc "#E12C80" title "Average"
