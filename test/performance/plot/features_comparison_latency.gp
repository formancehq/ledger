
set terminal cgm width 10/2.54*72
set term png

set output "{{.OutputFile}}"
set boxwidth 0.9 relative
set style data histograms
set style histogram rowstacked
set style fill solid 1.0 border lt -1
set xtics rotate by 90 right

plot for [COL=2:3] '{{.Datafile}}' using COL:xticlabels(1) title col
