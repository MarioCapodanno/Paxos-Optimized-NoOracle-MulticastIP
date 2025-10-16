datafile="logs/latency_client1"
output="logs/plot_cdf.pdf"

# General plot appearence
set terminal pdfcairo size 8,5 enhanced color font "Times-Roman,30"
set output output

set title "Latency CDF"
set xlabel "Latency (us)"
set xtics font ",25"
set ytics font ",25"
set xtics rotate by 45 right
set grid xtics ytics

set key font ",25"
set key right bottom

# Data selection
set yrange [0:1]

# Plot
sample_count = int(system(sprintf("wc -l %s", datafile))) - 1
plot datafile using ($1):(1.0/sample_count) smooth cumulative with lines lw 3 lc "#112C80" title "cdf"
