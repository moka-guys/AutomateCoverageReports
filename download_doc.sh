# source environment
source /etc/profile.d/dnanexus.environment.sh.off

# dx select project with api key
dx select NGS_runs --auth-token rsivxAMylcfpHvIIcZy8hDsFUVyVtvUL

runfolder=$1
mkdir -p ~/CoverageReportsDownloads/$runfolder
cd ~/CoverageReportsDownloads/$runfolder

# given runfolder
dx cd $runfolder
dx cd QC

#get a list of all depth of coverage_files
files=($(dx ls *sample_gene_summary))
#loop through bash script
for x in "${files[@]}"; do dx download $x; done

