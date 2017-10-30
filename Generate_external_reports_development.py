'''
Created on 18 Nov 2016

@author: aled
'''
import sys
import getopt
import os
import pyodbc
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import pdfkit
import datetime
import numpy


class test_input():

    '''script should receive an argument -p or --path which contains the path to the depth of coverage files.'''

    def __init__(self):
        # args
        self.usage = "python Generate_external_reports.py -t <NGSTestId> -p <panel1> -q <panel2> -r <panel3>"

        # variables for the database connection
        #self.cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=GSTTV-MOKA; DATABASE=devdatabase;")
        self.cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=GSTTV-MOKA; DATABASE=mokadata;")
        self.cursor = self.cnxn.cursor()

        # dictionary to hold the depth of coverage result for each dna number.
        self.coverage_dictionary = {}

        # variables to hold queries and exceptions
        self.select_qry = ""
        self.insert_query = ""
        self.select_qry_exception = ""
        self.backup_qry = ""

        # variable to inform an if loop
        self.gene = False

        # variables to capture info from moka
        self.runfolder = ""
        self.InternalPatientID = ""
        self.NGSTestID = None

        self.panel1 = None
        self.panel2 = None
        self.panel3 = None
        
        self.string_of_panels = "("
        self.report_panels="("
        
        self.mokapipeversion=""

        # path to html template
        self.html_template = "F:\\Moka\\Files\\Software\\depthofcoverage\\AutomateCoverageReports\\html_template\\"
        self.output_html = "S:\\Genetics\\Bioinformatics\\NGS\\depthofcoverage\\pdf_holding_area\\"
        
        self.path_wkthmltopdf = r'S:\Genetics_Data2\Array\Software\wkhtmltopdf\bin\wkhtmltopdf.exe'
        self.config = pdfkit.configuration(wkhtmltopdf=self.path_wkthmltopdf)
        
        self.warning = True
        
    def capture_NGSTestID(self, argv):
        ''' capture the command line arguments'''
        # print argv

        try:
            opts, args = getopt.getopt(argv, "t:p:q:r:", ["NGSTestId=", "panel1", "panel2", "panel3"])
        except getopt.GetoptError:
            print "ERROR", self.usage
            sys.exit(2)

        # if help argument print usage otherwise capture path argument
        for opt, arg in opts:
            if opt == 'h':
                print self.usage
                sys.exit()
            elif opt in ("-t", "--NGSTestId"):
                self.NGSTestID = arg
                # print "NGS TEST ID FOUND", self.NGSTestID
            elif opt in ("-p", "--panel1"):
                self.panel1 = arg
                # print "PANEL1=",str(self.panel1)
            elif opt in ("-q", "--panel2"):
                self.panel2 = int(arg)
            elif opt in ("-r", "--panel3"):
                self.panel3 = int(arg)

    def create_coverage_report(self):
        ''' This function creates a coverage report for each sample using the NGSTestID and  '''
        # build a string of panels passed to program for sql query
        if self.panel1:
            self.string_of_panels = self.string_of_panels + str(self.panel1)
            self.report_panels = self.report_panels + "Pan" +str(self.panel1)
        if self.panel2:
            if self.panel1:
                self.string_of_panels = self.string_of_panels + "," + str(self.panel2)
                self.report_panels = self.report_panels + ", Pan" + str(self.panel2)
            else:
                self.string_of_panels = self.string_of_panels + str(self.panel2)
                self.report_panels = self.report_panels + "Pan" +str(self.panel2)
        if self.panel3:
            if self.panel1 or self.panel2:
                self.string_of_panels = self.string_of_panels + "," + str(self.panel3)
                self.report_panels = self.report_panels + ", Pan" + str(self.panel3)
            else:
                self.string_of_panels = self.string_of_panels + str(self.panel3)
                self.report_panels = self.report_panels + "Pan" +str(self.panel3)

        self.string_of_panels = self.string_of_panels + ")"
        self.report_panels = self.report_panels + ")"
        
        # print string_of_panels
        self.select_qry = "select distinct dbo.GenesHGNC_current_translation.ApprovedSymbol,dbo.NGSCoverage.avg_coverage,dbo.NGSCoverage.above20X \
        from dbo.NGSPanelGenes, dbo.GenesHGNC_current_translation, dbo.NGSCoverage \
        where dbo.NGSPanelGenes.NGSPanelID in " + self.string_of_panels + " and dbo.GenesHGNC_current_translation.EntrezId_PanelApp=dbo.NGSCoverage.GeneSymbol and dbo.GenesHGNC_current_translation.HGNCID=dbo.NGSPanelGenes.HGNCID and dbo.NGSCoverage.NGSTestID = "+self.NGSTestID
        #where dbo.NGSPanelGenes.NGSPanelID in " + self.string_of_panels + " and dbo.GenesHGNC_current_translation.RefSeqGeneSymbol=dbo.NGSCoverage.GeneSymbol and dbo.GenesHGNC_current_translation.HGNCID=dbo.NGSPanelGenes.HGNCID and dbo.NGSCoverage.NGSTestID = "+self.NGSTestID
        
        
        # print self.select_qry
        self.select_qry_exception = "Can't pull out the coverage for NGS test" + str(self.NGSTestID)+". query is: "+ self.select_qry
        coverage_result = self.select_query()


        # test for the number of genes in the panels
        self.select_qry = "select distinct Symbol from dbo.NGSPanelGenes where NGSPanelID in "+self.string_of_panels
        self.select_qry_exception ="Can't pull out number of genes in the panels from NGSPanelGenes. query is: " + self.select_qry  
        expected_genes=self.select_query()
        
        
        
        
        # check that the number of genes in coverage result is the same number of genes that we would expect that from the gene panels that are also in Pan493.  
        if len(expected_genes)!=len(coverage_result):
            # pull out genes from the gene panels which are NOT in Pan493.
            self.select_qry = "select distinct HGNCID from dbo.NGSPanelGenes where NGSPanelID in "+self.string_of_panels + " and HGNCID not in (select HGNCID from dbo.NGSPanelGenes where NGSPanelID = 493)"
            self.select_qry_exception ="Can't pull out number of genes in the panels which are also in Pan493: " + self.select_qry
            self.warning = False 
            phenotype_locus_genes=self.select_query()
            self.warning = True
            #print "phenotype locus:"+str(len(phenotype_locus_genes))
            #print "coverage result:"+str(len(coverage_result))
            #print "expected count:"+str(len(expected_genes))
            
            if len(expected_genes)==len(coverage_result)+len(phenotype_locus_genes):
                print "WARNING:"+str(len(phenotype_locus_genes))+" genes are present in the gene panel(s) which are not in Pan493 (the exome bed file). These genes will not be present in the coverage report. Please ask the bioinformatics team to identify these genes if required."
            else:
                print "len(expected_genes))"+str(len(expected_genes))
                print "len(coverage_result)"+str(len(coverage_result))
                print "len(phenotype_locus_genes)"+str(len(phenotype_locus_genes))
                               
                # troubleshooting query to find genes not in Pan493
                troubleshooting="select dbo.GenesHGNC_current_translation.HGNCID, LocusType,ApprovedSymbol from dbo.NGSPanelGenes,dbo.GenesHGNC_current_translation where dbo.GenesHGNC_current_translation.HGNCID = dbo.NGSPanelGenes.HGNCID and NGSPanelID in "+self.string_of_panels + " and dbo.GenesHGNC_current_translation.HGNCID not in (select HGNCID from dbo.NGSPanelGenes where NGSPanelID = 493)
                print troubleshooting
                
                raise Exception("There is a gene in the gene panel which is not in the coverage report. Please ask a Bioinformatician to identify the gene(s)")
                
        # print coverage_result
        for_df = {}
        if coverage_result is not None:
            for gene in coverage_result:
                for_df[gene[0]] = [gene[2]]

            df = pd.DataFrame.from_dict(for_df, orient='index')
            df.columns = ['Percentage Bases at 20X*']
            df.sort_index(inplace=True)
            # print df

            env = Environment(loader=FileSystemLoader(self.html_template))
            template = env.get_template("internal_report_template.html")

            #self.select_qry = "select BookinLastName,BookinFirstName,BookinDOB,'MALE',PatientID, dna, item from dbo.NGSTest, dbo.Patients, dbo.Item where BookinSex = 'M' and dbo.Item.ItemID=dbo.NGSTest.pipelineversion and dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID and NGSTestID = " + str(self.NGSTestID) + " union select BookinLastName,BookinFirstName,BookinDOB,'FEMALE',PatientID,dna, item from dbo.NGSTest, dbo.Patients, dbo.Item where BookinSex = 'F' and dbo.Item.ItemID=dbo.NGSTest.pipelineversion and dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID and NGSTestID = " + str(self.NGSTestID)
            self.select_qry = "select BookinLastName,BookinFirstName,BookinDOB,'MALE',PatientID, dna, item  from dbo.NGSTest, dbo.Patients, dbo.Item where BookinSex = 'M' and dbo.Item.ItemID=dbo.NGSTest.pipelineversion and dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID and NGSTestID = " + str(self.NGSTestID) + " union select BookinLastName,BookinFirstName,BookinDOB,'FEMALE',PatientID,dna , item from dbo.NGSTest, dbo.Patients, dbo.Item where BookinSex = 'F' and dbo.Item.ItemID=dbo.NGSTest.pipelineversion and dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID and NGSTestID = " + str(self.NGSTestID) + " union select BookinLastName,BookinFirstName,BookinDOB,'UNKNOWN',PatientID, dna , item from dbo.NGSTest, dbo.Patients, dbo.Item where BookinSex != 'F' and BookinSex != 'M' and dbo.Item.ItemID=dbo.NGSTest.pipelineversion and dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID and NGSTestID = "+ str(self.NGSTestID)

            self.select_qry_exception = "Can't pull out the patient info for NGSTestID " + str(self.NGSTestID) + ". Bookinsex must be F or M, an NGSTestID must be present and joins are dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID "
            ID = self.select_query()
            PRU = ID[0][4]
            PRU_for_pdfname=PRU.replace(":","")
            Name = str.upper(ID[0][0]) + " " + ID[0][1]
            Gender = ID[0][3]
            DoB = ID[0][2]
            DNAnumber=ID[0][5]
            if DoB:
                format = "%d/%m/%Y"
                DoB = DoB.strftime(format)
            else:
                DoB = ""
            self.mokapipeversion=ID[0][6]
            
            html_table="<table border=\"1\" width=\"60%\" cellpadding=\"3\" cellspacing=\"0\">\n\
            \t<thead>\n\
            \t<tr style=\"text-align: centre;\" bgcolor=\"#A8A8A8\">\n\
            \t\t<th>Gene</th>\n\
            \t\t<th>Percentage Bases at 20X*</th>\n\
            \t</tr>\n\
            </thead>\n\
            <tbody>\n"
            
            for index, row in df.iterrows():
                gene=index
                coverage=str(int(numpy.floor(row['Percentage Bases at 20X*'])))
                #print gene
                
                html_table=html_table+"\t\t\t\t<tr align=\"center\">\n\
                \t<td>"+gene+"</td>\n\
                \t<td>"+coverage+"</td>\n\
                </tr>\n"
            
            html_table=html_table+"</tbody>\n</table>"
             
            template_vars = {"coverage_table":html_table, "panellist":self.report_panels, "PRU": PRU, "Name": Name, "Gender": Gender, "DoB": DoB, "MokaPipe_version":self.mokapipeversion}
             
            #print template_vars["coverage_table"]
            with open(self.output_html + str(self.NGSTestID) + ".html", "wb") as fh:
                fh.write(template.render(template_vars))


            options={'footer-right':'Page [page] of [toPage]','footer-left':'Date Created [isodate]','quiet':""}
            pdfkit.from_file(self.output_html + str(self.NGSTestID) + ".html", self.output_html + str(PRU_for_pdfname)+"." +str(DNAnumber)+ ".cov.pdf", configuration=self.config,options=options)
            print "Report can be found @ S:\Genetics\DNA LAB\R&D\New Tests\WES\Results\Coverage reports"
            
    def select_query(self):
        '''This function is called to retrieve the whole result of a select query '''
        # Perform query and fetch all
        result = self.cursor.execute(self.select_qry).fetchall()
        
        # return result
        if result:
            return(result)
        else:
            if self.warning:
                raise Exception(self.select_qry_exception)
            else:
                result=[]
                return result

    def insert_query_function(self):
        '''This function executes an insert query'''
        # execute the insert query
        self.cursor.execute(self.insert_query)
        self.cursor.commit()

if __name__ == '__main__':
    a = test_input()
    a.capture_NGSTestID(sys.argv[1:])
    a.create_coverage_report()
