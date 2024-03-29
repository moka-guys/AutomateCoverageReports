'''
Created on 18 Nov 2016
This script generates a coverage report in a pdf file.
The script is invoked with the NGS TestID and up to three gene panels which are to be included in the report.
The coverage data (generated by sambamba in mokapipe) must have been imported in Moka.
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
from ConfigParser import ConfigParser


class GenerateCoverageReport():
    def __init__(self):
        """Set all parameters used later in the script"""

        # define the expected usage to return incase of error
        self.usage = "python Generate_external_reports.py -t <NGSTestId> -p <PrimaryPanel> -q <SecondaryPanel> -r <TertiaryPanel>\nAt least one of -p, -q or -r is required."

		# Read config file (must be called config.ini and stored in same directory as script)
        config = ConfigParser()
        config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.ini"))

        # variables for the database connection
        self.cnxn = pyodbc.connect('DRIVER={{SQL Server}}; SERVER={server}; DATABASE={database};'.format(
            server=config.get("MOKA", "SERVER"),
            database=config.get("MOKA", "DATABASE")
            ), 
            autocommit=True
        )

        # create an object for the database connection
        self.cursor = self.cnxn.cursor()

        # variables to hold queries and exceptions
        self.select_query = ""
        self.select_query_exception = ""

        # variable to hold the NGS test ID
        self.ngs_test_id = None

        #variables to hold the panel numbers given as arguments to this script
        self.primary_panel = None
        self.secondary_panel = None
        self.tertiary_panel = None

        # variable to build a string of all the panel codes used in a sql query
        self.string_of_panels = "("
        # variable to build a string of all the panel codes to be stated on the report
        self.report_panels="("
        # variable to hold the mokapipe verison
        self.mokapipe_version=""

        # path to html templates used by pdfkit
        self.html_template = "\\\\gstt.local\\Apps\\Moka\\Files\\Software\\depthofcoverage\\AutomateCoverageReports\\html_template\\"
        self.output_html = "\\\\gstt.local\\shared\\Genetics\\Bioinformatics\\NGS\\depthofcoverage\\pdf_holding_area\\"
        # path to wkhtmltopdf.exe. This software is used to convert html to a pdf - note the r before path required because of windows (I think)
        self.wkthmltopdf_path = r'\\gstt.local\shared\Genetics_Data2\Array\Software\wkhtmltopdf\bin\wkhtmltopdf.exe'
        # set the config for pdfkit  - specifying where the wkhtmltopdf.exe can be found
        self.pdfkit_config = pdfkit.configuration(wkhtmltopdf=self.wkthmltopdf_path)

        # a flag used to detemine whether to error if a sql query returns no results (this is expected in some cases but should be flagged in other cases)
        self.warning = True
        
    def capture_command_line_arguments(self, argv):
        """ This function receives the command line arguments (NGS TestID and gene panels) as an input (argv).
        The function parses this and populates the self.variables
        """

        # using a try except loop and the getopt package go through this list of pairs of values
        try:
            # the get opt package requires the inputs to be defined. these can be in a short format (a single character) and a longer format.
            # If an argument has a input value (as opposed to a on/off flag) this is stated by the presense of a colon following the short format of argument names.
            # the longer format of argument names is optional
            # the getopt.getopt function creates a list of tuples for all inputs.
            opts, args = getopt.getopt(argv, "ht:p:q:r:", ["NGSTestId", "PrimaryPanel", "SecondaryPanel", "TertiaryPanel"])
        except getopt.GetoptError:
            # if any issue getting arguments print the expected usage
            print "ERROR", self.usage
            # and exit with a error status of 2, denoting a command line syntax error
            sys.exit(2)

        # go through the list of inputs (a tuple for each input)
        for flag,value  in opts:
            # if the flag h is found print the usage (note h isn't defined as an input above)
            if flag == '-h':
                print self.usage
                # exit the script
                sys.exit()
            #capture the NGS test ID attached to the -t flag
            elif flag in ("-t", "--NGSTestId"):
                self.ngs_test_id = value
                # print "NGS TEST ID FOUND", self.ngs_test_id
            # capture PrimaryPanel (-p flag)
            elif flag in ("-p", "--PrimaryPanel"):
                self.primary_panel = value
                # print "PANEL1=",str(self.primary_panel)
            # capture SecondaryPanel (-q flag)
            elif flag in ("-q", "--SecondaryPanel"):
                self.secondary_panel = int(value)
            # capture TertiaryPanel (-r flag)
            elif flag in ("-r", "--TertiaryPanel"):
                self.tertiary_panel = int(value)


    def extract_coverage_data(self):
        """ This function creates a coverage report for each sample using the NGSTestID and any given panels"""

        # check at least one panel has been set
        if not any((self.primary_panel,self.secondary_panel,self.tertiary_panel)):
            # if no panels have been set print usage
            print "no gene panels have been provided." + self.usage
            # and stop the script
            sys.exit()

        # The panel numbers are combined into a SQL list to be used in the SQL query.
        # any combination of -p, -q and -r may be present.
        # if PrimaryPanel is present (-p) 
        if self.primary_panel:
            # add the panel number to the SQL string (self.string_of_panels)
            self.string_of_panels = self.string_of_panels + str(self.primary_panel)
            # also add the panel number to the text string on the report)
            self.report_panels = self.report_panels + "Pan" + str(self.primary_panel)
        # if SecondaryPanel is present (-q)
        if self.secondary_panel:
            # if PrimaryPanel has already been added to the string need to add this to the string with a comma
            if self.primary_panel:
                # add the panel number to the SQL string (self.string_of_panels) after a comma
                self.string_of_panels = self.string_of_panels + "," + str(self.secondary_panel)
                # also add the panel number to the text string on the report) after a comma
                self.report_panels = self.report_panels + ", Pan" + str(self.secondary_panel)
            # if this is the first panel number to be added to the string
            else:
                # add the panel number to the SQL string (self.string_of_panels) (no comma needed)
                self.string_of_panels = self.string_of_panels + str(self.secondary_panel)
                # also add the panel number to the text string on the report) no comma needed
                self.report_panels = self.report_panels + "Pan" +str(self.secondary_panel)
        # if tertiaryPanel is present (-r)
        if self.tertiary_panel:
            # if either PrimaryPanel or SecondaryPanel is in the list
            if self.primary_panel or self.secondary_panel:
                # add the panel number to the SQL string (self.string_of_panels) after a comma
                self.string_of_panels = self.string_of_panels + "," + str(self.tertiary_panel)
                # also add the panel number to the text string on the report) after a comma
                self.report_panels = self.report_panels + ", Pan" + str(self.tertiary_panel)
            else:
                # add the panel number to the SQL string (self.string_of_panels) (no comma needed)
                self.string_of_panels = self.string_of_panels + str(self.tertiary_panel)
                # also add the panel number to the text string on the report (no comma needed)
                self.report_panels = self.report_panels + "Pan" +str(self.tertiary_panel)

        # close the brackets on the lists
        self.string_of_panels = self.string_of_panels + ")"
        self.report_panels = self.report_panels + ")"
        
        # The query to pull out the coverage data using the list of panels and the ngs test id is: 
        self.select_query = "select distinct dbo.GenesHGNC_current.ApprovedSymbol,dbo.NGSCoverage.avg_coverage,dbo.NGSCoverage.above20X \
        from dbo.NGSPanelGenes, dbo.GenesHGNC_current, dbo.NGSCoverage \
        where dbo.NGSPanelGenes.NGSPanelID in " + self.string_of_panels + " and dbo.GenesHGNC_current.EntrezGeneIDmapped = dbo.NGSCoverage.GeneSymbol and dbo.GenesHGNC_current.HGNCID=dbo.NGSPanelGenes.HGNCID and dbo.NGSCoverage.NGSTestID = " + self.ngs_test_id

        # the exception message to be printed should the select query fail includes the query that was executed 
        self.select_query_exception = "Can't pull out the coverage for NGS test" + str(self.ngs_test_id) + ". query is: " + self.select_query
        
        # call the self.select query which uses the above queries.
        # assign the result (a list of tuples) to the variable coverage_result
        coverage_result = self.perform_select_query()
        # return the result
        return coverage_result

    def test_result(self,coverage_data):
        """
        A number of tests are required to ensure that all the genes on the gene panel for which a coverage value is generated is included on the report (and only genes from the gene panels are on the report) 
        """
        
        # count the distinct number of genes in the panels.
        self.select_query = "select distinct HGNCID from dbo.NGSPanelGenes where NGSPanelID in " + self.string_of_panels
        self.select_query_exception = "Can't pull out number of genes in the panels from NGSPanelGenes. query is: " + self.select_query  
        
        # use self.select_query to execute the select query
        expected_genes = self.perform_select_query()
        
        # check that the number of genes in coverage result is the same as the number of genes in the gene panels  
        if len(expected_genes) != len(coverage_data):
            # If this number is different it may be that there are genes on the gene panels which are NOT in Pan493 (therefore no coverage data is generated).
            # these genes may be phenotype only or immunoglobulin genes for which there are no coordinates
            
            # pull out any genes from the gene panels where the gene is not in Pan493
            self.select_query = "select distinct HGNCID from dbo.NGSPanelGenes where NGSPanelID in " + self.string_of_panels + " and HGNCID not in (select HGNCID from dbo.NGSPanelGenes where NGSPanelID = 493)"
            self.select_query_exception = "Can't pull out number of genes in the panels which are also in Pan493: " + self.select_query
            # this query may return an empty result. The self.select_query() will raise an error unless the self.warning flag is turned off. 
            # turn off warning flag
            self.warning = False
            # execute the query 
            genes_not_in_493 = self.perform_select_query()
            # turn warning back on
            self.warning = True
                        
            # check the discrepancy between expected genes and genes on coverage report can be explained by genes on the gene panel that aren't in Pan493
            if len(expected_genes) == len(coverage_data) + len(genes_not_in_493):
                # print a warning to say there were genes on the panel for which coverage was not generated
                print "WARNING:" + str(len(genes_not_in_493)) + " genes are present in the gene panel(s) which are not in Pan493 (the exome bed file). These genes will not be present in the coverage report. Please ask the bioinformatics team to identify these genes if required."
            else:
                # otherwise there is a more tricky problem to solve
                # print how many genes we can't account for
                print "len(expected_genes))" + str(len(expected_genes))
                print "len(coverage_result)" + str(len(coverage_data))
                print "len(genes_not_in_493)" + str(len(genes_not_in_493))

                # This query may be useful when troubleshooting  - it states the genes which are not in Pan493
                troubleshooting = "select distinct dbo.GenesHGNC_current.HGNCID, LocusType,ApprovedSymbol from dbo.NGSPanelGenes,dbo.GenesHGNC_current where dbo.GenesHGNC_current.HGNCID = dbo.NGSPanelGenes.HGNCID and NGSPanelID in " + self.string_of_panels + " and dbo.GenesHGNC_current.HGNCID not in (select HGNCID from dbo.NGSPanelGenes where NGSPanelID = 493)"
                # print query so it can be copied and pasted with the gene panels etc listed 
                print troubleshooting
                # print further clues to aid troublsehooting
                print "this error may also be due to different gene symbols for the same HGNCID in the NGSpanelgenes table"   
                
                # raise an exception to tell the user what to do.
                raise Exception("There is a gene in the gene panel which is not in the coverage report. Please ask a Bioinformatician to identify the gene(s)")
        
        # if no exceptions have been raised the coverage report contains all and only the expected genes and the test has passed
        return True
    
    def generate_report(self, coverage_data):
        """
        The coverage data has been extracted from the database and tested to ensure completeness
        The data must now be formated into a report. 
        """ 
        
        # create a dictionary to be populated with coverage data
        coverage_dictionary = {}
        # if coverage data is not empty
        if coverage_data is not None:
            # loop through each gene
            for gene in coverage_data:
                # add the approved symbol as a key, and the % above 20X as the value
                coverage_dictionary[gene[0]] = [gene[2]]
            
            # put this dictionary into a pandas data frame, with the gene symbol as the index 
            coverage_data_frame = pd.DataFrame.from_dict(coverage_dictionary, orient='index')
            
            # label the column 
            coverage_data_frame.columns = ['Percentage Bases at 20X*']
            
            # sort the dataframe so the genes appear in alphabetical order
            coverage_data_frame.sort_index(inplace=True)

            # extract patient demographics to populate the report             
            self.select_query = "select BookinLastName,BookinFirstName,BookinDOB,'MALE',PatientID, dna, item, nhsnumber from dbo.NGSTest, dbo.Patients, dbo.Item where BookinSex = 'M' and dbo.Item.ItemID=dbo.NGSTest.pipelineversion and dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID and NGSTestID = " + str(self.ngs_test_id) + " union select BookinLastName,BookinFirstName,BookinDOB,'FEMALE',PatientID,dna,item, nhsnumber from  dbo.NGSTest, dbo.Patients, dbo.Item where BookinSex = 'F' and dbo.Item.ItemID=dbo.NGSTest.pipelineversion and dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID and NGSTestID = " + str(self.ngs_test_id) + " union select BookinLastName,BookinFirstName,BookinDOB,'UNKNOWN',PatientID, dna , item, nhsnumber from  dbo.NGSTest, dbo.Patients, dbo.Item where BookinSex != 'F' and BookinSex != 'M' and dbo.Item.ItemID=dbo.NGSTest.pipelineversion and dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID and NGSTestID = "+ str(self.ngs_test_id)
            # set the exception message to print should the patient not be found
            self.select_query_exception = "Can't pull out the patient info for NGSTestID " + str(self.ngs_test_id) + ". An NGSTestID must be present and joins are dbo.Patients.InternalPatientID=dbo.NGSTest.InternalPatientID. If mokapipe version is null it may be a re-analysis case?"
            # execute the query
            id = self.perform_select_query()
            
            # capture each item from the query 
            pru = id[0][4]
            # the pru is also used to name the report, however the colon must be removed
            pru_for_pdfname = pru.replace(":", "")
            # Create the footer text
            pru_footer="PRU:"+pru
            # concatenate the last and first names, capitalising the surname 
            name = str.upper(id[0][0]) + " " + id[0][1]
            # Create the footer text
            name_footer="\nName:"+name
            # capture gender
            gender = id[0][3]
            # set date of birth
            date_of_birth = id[0][2]
            # set dna number
            dna_number=id[0][5]
            # nhs number
            nhs_num=id[0][7]
            # Create the footer text - NHS number isn't always present so put this in an if loop
            if nhs_num:
                nhs_num_footer="NHS number:"+nhs_num
            else:
                nhs_num_footer="NHS number:"
            # convert the date of birth into a specific format eg 25/12/2017
            if date_of_birth:
                format = "%d/%m/%Y"
                date_of_birth = date_of_birth.strftime(format)
            else:
                # if no date of birth can be found, set an empty string
                date_of_birth = ""
            # set the verison of mokapipe to be stated on report
            self.mokapipe_version = id[0][6]
            
            # The coverage information is displayed on a table.
            # As the coverage report can be any length the html table is created in this script as opposed to the template.
            # The template has a placeholder where this table html code is placed when the report is rendered 
            # create the start of the table, and state the table headers.
            html_table = "<table border=\"1\" width=\"60%\" cellpadding=\"3\" cellspacing=\"0\">\n\
            \t<thead>\n\
            \t<tr style=\"text-align: centre;\" bgcolor=\"#A8A8A8\">\n\
            \t\t<th>Gene</th>\n\
            \t\t<th>Percentage Bases at 20X*</th>\n\
            \t</tr>\n\
            </thead>\n\
            <tbody>\n"
            
            # then loop through each item in the pandas dataframe, capturing the index (gene symbol) and the row
            for gene, row in coverage_data_frame.iterrows(): 
                # pull out the percentage Bases at 20X column from that row, and round down to the nearest whole number eg 99.9 -> 99(to ensure coverage is not over stated)
                coverage = str(int(numpy.floor(row['Percentage Bases at 20X*'])))
                # add a row the the html table for the gene and coverage
                html_table = html_table+"\t\t\t\t<tr align=\"center\">\n\
                \t<td>" + gene + "</td>\n\
                \t<td>" + coverage + "</td>\n\
                </tr>\n"
            # when all genes have been added to the table close the table. 
            html_table = html_table + "</tbody>\n</table>"
            
            # specify the folder containing the html templates 
            html_template_dir = Environment(loader=FileSystemLoader(self.html_template))
            # specify which html template to use, and load this as a python object, template
            html_template = html_template_dir.get_template("internal_report_template.html")
            
            # the template has a number of placeholders. When the report is rendered these are populated from a dictionary
            # set the value of each placeholder in the template  
            place_holder_values = {"coverage_table":html_table, "panellist":self.report_panels, "PRU": pru, "Name": name, "Gender": gender, "DoB": date_of_birth, "MokaPipe_version":self.mokapipe_version}
            
            # open a html file, saved under the NGS TestID
            with open(self.output_html + str(self.ngs_test_id) + ".html", "wb") as html_file:
                # write a copy of the template, filling the placeholders using the dictionary
                html_file.write(html_template.render(place_holder_values))

            # the html file can then be converted into a PDF. This uses the pdfkit package and wkhtmltopdf software (specified in __init__)
            # a number of options can be added, such as footers on the page and any standard out when generating the report
            # add page number and date stamp to report and turn off any standard out (this would be displayed in the message box in moka, if the report is generated in moka)
            pdfkit_options = {'footer-left':pru_footer+name_footer,'footer-center':nhs_num_footer,'footer-right':'Page [page] of [toPage]\nDate Created [isodate]','quiet':""}
            # using the pdfkit package, specify the html file to be converted, name the pdf kit using the PRU and DNA number and pass in the software locations and options stated above 
            pdfkit.from_file(self.output_html + str(self.ngs_test_id) + ".html", self.output_html + str(pru_for_pdfname) + "." + str(dna_number) + ".NGSTestID_" + str(self.ngs_test_id) + ".cov.pdf", configuration=self.pdfkit_config,options=pdfkit_options)
            # report to the user where the reports can be found (NB this location is different to where the reports are saved to - these are either moved manually or in the Moka front end)
            print "Report can be found @ S:\Genetics\DNA LAB\Current\WES\Coverage reports"
            
    def perform_select_query(self):
        """This function is called to retrieve the whole result of a select query """
        # Perform query and fetch all
        result = self.cursor.execute(self.select_query).fetchall()
        
        # if the query returned any results return them
        if result:
            return(result)
        # if no results were found
        else:
            # and the warning flag is on raise the given exception message which should help troubleshooting
            if self.warning:
                raise Exception(self.select_query_exception)
            # if the warning flag is not set return an empty list
            else:
                result = []
                return result

def main():
    """
    This function uses the above functions to generate a report
    """
    # create an instance of the class
    CreateReport = GenerateCoverageReport()
    # send all command line arguments, except the name of the script to be assigned to variables
    CreateReport.capture_command_line_arguments(sys.argv[1:])
    # using the captured command line arguments extract the coverage data
    coverage_data = CreateReport.extract_coverage_data()
    # perform a test, to ensure that the report contains all the required genes, and no additional genes
    if CreateReport.test_result(coverage_data):
        # if the test passed create the html file and convert to pdf 
        CreateReport.generate_report(coverage_data)
    
if __name__ == '__main__':
    # call main class
    main()
    
