'''
Created on 18 Nov 2016
@author: aled
'''
import sys
import getopt
import os
import pyodbc


class test_input():

    '''script should receive an argument -d or --dnanumber which contains the path to the depth of coverage files.'''

    def __init__(self):
        self.folder_path = "S:\\Genetics\\Bioinformatics\\NGS\\depthofcoverage\\genesummaries\\"
        #self.folder_path = "S:\\Genetics_Data2\\Array\\Audits and Projects\\161118 automatecoverage\\chanjo_out\\"
        self.usage = "python import_depth_of_coverage.py -d <dnanumber>"

        # self.dnanumber
        self.dnanumber = ""

        # variables for the database connection
        self.cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=GSTTV-MOKA; DATABASE=mokadata;")
        #self.cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=GSTTV-MOKA; DATABASE=devdatabase;")
        self.cursor = self.cnxn.cursor()

        # dictionary to hold the depth of coverage result for each dna number.
        self.coverage_dictionary = {}

        # variables to hold queries and exceptions
        self.select_qry = ""
        self.insert_query = ""
        self.select_qry_exception = ""
        self.backup_qry = ""

        # variables to capture info from moka
        self.runfolder = ""
        self.InternalPatientID = ""
        self.NGSTestID = ""


    def set_depth_of_coverage_path(self, argv):
        ''' capture the command line arguments'''
        # look for path argument
        try:
            opts, args = getopt.getopt(argv, "d:", ["dnanumber="])
        except getopt.GetoptError:
            print self.usage
            sys.exit(2)

        # if help argument print usage otherwise capture path argument
        for opt, arg in opts:
            if opt == 'h':
                print self.usage
                sys.exit()
            elif opt in ("-d", "--dnanumber"):
                self.dnanumber = arg

    def read_depth_of_coverage_files(self):
        '''using the path argument loop through all the depth of coverage files in folder. 
        Create a dictionary entry within self.coverage dict for each dna number. 
        each dnanumber there is another dictionary with the gene as a key and coverage as value'''
        # for DoC file in folder
        for file in os.listdir(self.folder_path):
            if file.startswith("imported"):
                pass
            else:
                # capture info from the filename
                filename = file.split("_")
                DNAnumber = filename[2]
                # print self.dnanumber
                if DNAnumber == self.dnanumber:
                    # print file
                    # create an empty dict for this DNA number
                    self.coverage_dictionary[DNAnumber] = {}
    
                    # open and loop through file
                    gene_summary_file = open(self.folder_path + "/" + file, 'r')
                    for line in gene_summary_file:
                        # ignore header
                        if line.startswith("Gene"):  # or line.startswith("UNKNOWN") or line.startswith("LOC100287896") or line.startswith("LOC81691")or line.startswith("TARP"):
                            pass
                        else:
                            # capture gene, avg coverage and coverage @ 20X
                            splitline = line.split('\t')
                            gene = splitline[0]
                            avg_coverage = splitline[2]
                            coverage20x = splitline[1]
                            # put this tuple into dict
                            self.coverage_dictionary[DNAnumber][gene] = ((avg_coverage, coverage20x.rstrip()))


    def insert_to_db(self):
        '''Work through the self.coverage dict and extract identifiers from dna number. The '''
        # for each sample
        for dnanumber in self.coverage_dictionary:
            print "dnanumber = "+str(dnanumber)
            # capture the runfolder from the path
            runfolderpath = self.folder_path.split('\\')
            self.runfolder = runfolderpath[-1]

            # select query to find the internal patientid from dna number
            self.select_qry = "select InternalPatientID from dbo.DNA where DNANumber = '" + dnanumber + "'"
            # print self.select_qry
            self.select_qry_exception = "can't get the internalpatientID from dnanumber " + str(dnanumber)
            # capture the internal patientid
            self.InternalPatientID = self.select_query()[0][0]

            # Get the NGS test ID
            self.select_qry = "select NGSTestID from dbo.NGSTest where InternalPatientID=" + str(self.InternalPatientID)  # +" and StatusID != 4"
            self.select_qry_exception = "Can't pull out the NGS test ID for internal patient id " + str(self.InternalPatientID)
            NGSTestID = self.select_query()
            if NGSTestID:
                # ensure only one NGSTestID:
                assert len(NGSTestID) == 1
                self.NGSTestID = NGSTestID[0][0]
            else:
                raise Exception(self.select_qry_exception)

            # Ensure the NGS testID isn't in the coverage table already:
            self.select_qry = "select distinct NGSTestID from dbo.NGSCoverage"
            self.select_qry_exception = "Can't pull out any existing NGStestIDs from coverage table"
            existingNGSTestID = self.select_query()
            list_of_existing_testids = []
            if existingNGSTestID:
                for i in existingNGSTestID:
                    list_of_existing_testids.append(i[0])

            # if NGSTestID not already in coverage pass
            if self.NGSTestID in list_of_existing_testids:
                print "NGS TEST ALREADY IMPORTED"
            else:
                # for each gene capture elements of tuple
                for gene in self.coverage_dictionary[dnanumber]:
                    ApprovedSymbol = gene
                    avg_coverage = self.coverage_dictionary[dnanumber][gene][0]
                    above20x = self.coverage_dictionary[dnanumber][gene][1]
                    # insert gene to coverage table
                    self.insert_query = "insert into dbo.NGSCoverage (GeneSymbol,InternalPatientID,avg_coverage,above20X,DNAnumber,runfolder,NGSTestID) values ('" + str(ApprovedSymbol) + "'," + str(self.InternalPatientID) + "," + str(avg_coverage) + "," + str(above20x) + "," + str(dnanumber) + ",'" + self.runfolder + "'," + str(self.NGSTestID) + ")"
                    self.insert_query_function()

            # call function to make coverage report
            # self.get_panel()

    def select_query(self):
        '''This function is called to retrieve the whole result of a select query '''
        # Perform query and fetch all
        result = self.cursor.execute(self.select_qry).fetchall()

        # return result
        if result:
            return(result)
        else:
            raise Exception(self.select_qry_exception)

    def insert_query_function(self):
        '''This function executes an insert query'''
        # execute the insert query
        self.cursor.execute(self.insert_query)
        self.cursor.commit()


if __name__ == '__main__':
    a = test_input()
    a.set_depth_of_coverage_path(sys.argv[1:])
    a.read_depth_of_coverage_files()
    a.insert_to_db()
