# AutomateCoverageReports

The download_doc.sh script must be run using:

bash download_doc.sh runfolder
(NB bash not sh)

This script logs into the NGS_runs project and downloads all the files in the path $runfolder/qc that end in sample_gene_summary into the folder ~/CoverageReportsDownloads/$runfolder

It will then call a python script from that directory which will read each file and import to the moka database