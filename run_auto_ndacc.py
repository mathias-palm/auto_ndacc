import os, re, ftplib
import datetime
from pyhdf import SD
import sys
sys.path.append('/home/mathias/sfit-processing-environment/ModLib')
sys.path.append('/home/mathias/sfit-processing-environment/Lib_MP')
sys.path.append('/home/mathias/sfit-processing-environment/HDFsave')
import HDFmain_Bre,error_calc,create_hdf5

class run_auto_ndacc():

    def __init__(self, initfile):

        self.initfile = initfile
        fid = open(initfile)
        self.direc = []
        for l in fid:
            ll = l.split('#')[0]
            if len(ll.strip()) == 0:
                continue
            if len(ll.split('=')) == 2:
                if ll.split('=')[0].strip() == 'sfit4':
                    self.sfit4 = ll.split('=')[1].strip()
                if ll.split('=')[0].strip() == 'spec2asc':
                    self.spec2asc = ll.split('=')[1].strip()
                if l.split('=')[0].strip() == 'sbctl':
                    self.sbctl = ll.split('=')[1].strip()
                if ll.split('=')[0].strip() == 'hdfdir':
                    self.hdf_dir = ll.split('=')[1].strip()
                if ll.split('=')[0].strip() == 'retdir':
                    self.retdirtemplate = ll.split('=')[1].strip()
                if ll.split('=')[0].strip() == 'ptzdir':
                    self.ptz_dir = ll.split('=')[1].strip()
                if ll.split('=')[0].strip() == 'hkptemplate':
                    self.hkptemplate = ll.split('=')[1].strip()                    
                continue
            if len(self.direc) == 0:
                self.direc = [['%s'%i for i in ll.split()]]
            else:
                self.direc.append(['%s'%i for i in ll.split()])
            

                
        self.hdf_script_dir = '/home/mathias/sfit-processing-environment/HDFsave/'
        self.batch_sfit4 = '/home/mathias/bin/run_batch_sfit4_v0.9.6.6'
        self.active = -1
        self.sbdefaults = '/home/mathias/ndacc_sfit4/sbDefaults.ctl'
        
    def set_active(self,nr_active):
        self.active = nr_active
        direc = self.direc[self.active][0]
        self.location = self.direc[self.active][1]
        self.gas = self.direc[self.active][2]
        print(self.direc[nr_active])
        self.station = self.direc[nr_active][1]
        self.gas = self.direc[nr_active][2]
        # Change variables in the template
        with open(self.initfile) as fid:
            for l in fid:
                ll = l.split('#')[0]
                if len(ll.strip()) == 0:
                    continue
                if len(ll.split('=')) == 2:
                    # Set the filter for the retrievedgas
                    if ll.split('=')[0].strip() == self.gas.strip():
                        self.fltrset = ll.split('=')[1].strip() 
                    
        
        
    def setup_sunbatch(self):
        # set up hkpfile
        hkpfile = self.hkptemplate.replace('FLT',self.fltrset).replace('YEAR','{}'.format(self.year))
        self.ret_dir = self.retdirtemplate.replace('FLT',self.fltrset).replace('GAS',self.gas).replace('NR',str(self.active))
        
        sunbatch={'sfit4':self.sfit4}
        sunbatch.update({'hkpfile':hkpfile})
        sunbatch.update({'spc2asc':self.spec2asc})
        sunbatch.update({'ptzdir':self.ptz_dir})
        sunbatch.update({'retdir':self.ret_dir})
        self.modify_sunbatch(sunbatch)

        
        
    def modify_sunbatch(self, kwargs):
        sunbatch_keys = {'hkpfile':1,
                         'ptzdir':3,
                         'sfit4':10,
                         'retdir':13,
                         'spc2asc':14,
                         'daterange':21
                         }


        def replace_in_line(key, val):
            nr = sunbatch_keys[key]
            m = re.compile('^ *{}( |$)'.format(nr),re.I) # 1 is the key for the line containing the housekeeping
            ind = list(filter(lambda nr: m.search(lines[nr]), range(0,len(lines))))
            if len(ind) > 0:
                lines[ind[0]+1] = '{}\n'.format(kwargs[key])
            else:
                m = re.compile("^ *-1",re.I)
                ind = list(filter(lambda nr: m.search(lines[nr]), range(0,len(lines))))
                lines[ind[0]] = '{}\n'.format(nr)
                lines.append(kwargs[key])
                lines.append('\n')
                lines.append('-1')
        
        if self.active == -1:
            print('Set active retrieval first')
            return()
        direc = self.direc[self.active][0]
        fid = open('%s/%s'%(direc,'sun_batch.orig'))
        lines = fid.readlines()
        fid.close()
        for key in kwargs:
            replace_in_line(key,kwargs[key])
        fid = open('%s/%s'%(direc,'sun_batch.orig'), 'w')
        fid.writelines(lines)
        fid.close()


    def __find_last_retrievalday(self,out_dir):
        m = re.compile("[0-9]{8,8}?.[0-9]{6,6}?",re.I)
        datedirs = list(filter (m.search,os.listdir(out_dir)))
        datedirs.sort()
        self.ret_dates = datedirs
#        import ipdb
#        ipdb.set_trace()
        if len(datedirs) > 0:
               self.first_day = datetime.datetime.strptime(datedirs[-1],'%Y%m%d.%H%M%S') + datetime.timedelta(days=1)
        else:
               self.first_day = datetime.datetime.strptime('20180101.000000','%Y%m%d.%H%M%S')
        
    def retrieve(self,**kwargs):
        # nr_ret is the number of the parameters (see ctl file) 
        # retrieve all spectra from
        # start_date=YYYMMDD till end_date=YYYYMMDD
        # or if no start_date given with the day following the last successfully
	# retrieved spectrum till today if no end_date is given

        direc = self.direc[self.active][0]
        fid = open('%s/%s'%(direc,'sun_batch.orig'))
        lines = fid.readlines()
        fid.close()
        if self.active == -1:
            print('Set active retrieval first')
            exit
        kwargkeys = kwargs
        if 'start_date' not in kwargs:
            self.__find_last_retrievalday(self.ret_dir)
        else:
            self.first_day = datetime.datetime.strptime(kwargs['start_date'],'%Y%m%d')
        if 'end_date' not in kwargs:
            self.last_day = datetime.datetime.now() - datetime.timedelta(days=1)
        else:
            self.last_day = datetime.datetime.strptime(kwargs['end_date'],'%Y%m%d')
        m = re.compile("^ *21",re.I) # 21 is the key for the line containing the start and end date
        ind = list(filter(lambda nr: m.search(lines[nr]), range(0,len(lines))))
        if len(ind) > 0:
            lines[ind[0]+1] = '%s %s\n'%(self.first_day.strftime('%Y%m%d'),
                                          self.last_day.strftime('%Y%m%d'))
        else:
            m = re.compile("^ *-1",re.I)
            ind = list(filter(lambda nr: m.search(lines[nr]), range(0,len(lines))))
            print (self.first_day.strftime('%Y%m%d'))
            lines[ind[0]] = '21\n'
            lines.append('%s %s\n'%(self.first_day.strftime('%Y%m%d'),
                                    self.last_day.strftime('%Y%m%d')))
            lines.append('-1')

        fid = open('%s/%s'%(direc,'sun_batch'), 'w')
        fid.writelines(lines)
        fid.close()
        
        os.chdir(direc)
        if not 'run_sfit4' in kwargs or kwargs['run_sfit4']:
            os.system('{} < sun_batch'.format(self.batch_sfit4))

    def error_calc(self, **kwargs):

        if 'start_date' in kwargs:
            start_date = kwargs['start_date']
        else:
            start_date=self.first_day.strftime('%Y%m%d')
        if 'end_date' in kwargs:
            end_date = kwargs['end_date']
        else:
            end_date=self.last_day.strftime('%Y%m%d')
        error_calc.error_calc(dir=self.ret_dir,sbctl=self.sbctl,
                              start_date=start_date, end_date=end_date,
                              sbDefaults=self.sbdefaults)


        
    def create_tmph5(self,**kwargs):
        # Calculates error matrices and creates a tmp.h5 file
        # if the complete tmp.h5 is to be recalulated, give all_in_tmph5 as argumet
        if not 'all_in_tmph5' in kwargs:
            if 'start_date' in kwargs:
                start_date = kwargs['start_date']
            else:
                start_date=self.first_day.strftime('%Y%m%d')
            if 'end_date' in kwargs:
                end_date = kwargs['end_date']
            else:
                end_date=self.last_day.strftime('%Y%m%d')
        else:
            if kwargs['all_in_tmph5']:
                start_date='19000101'
                end_date='21001231'
        print(self.ret_dir)
        create_hdf5.create_hdf5(dir=self.ret_dir,sbctl=self.sbctl,start_date=start_date,end_date=end_date)
        print(self.ret_dir)

    def create_hdf(self, **kwargs):
        if self.active == -1:
            print('Set active retrieval first')
            exit
        location = self.direc[self.active][1]
        gas = self.direc[self.active][2]
        quality = self.direc[self.active][3]
        print(kwargs)
        if 'start_date' in kwargs:
            start_date = kwargs['start_date']
        else:
            start_date=self.first_day.strftime('%Y%m%d')
        if 'end_date' in kwargs:
            end_date = kwargs['end_date']
        else:
            end_date=self.last_day.strftime('%Y%m%d')
        if 'instrument' in kwargs:
            instrument = kwargs['instrument']
        else:
            instrument = 'misc'
        if 'hdf_dir' in kwargs:
            hdfdir = kwargs['hdf_dir']
        else:
            hdfdir = os.path.join(self.hdf_dir,self.gas)
        if 'quality' in kwargs:
            quality = kwargs['quality']
        else:
            print('Specify quality (FINAL or CAMS27)')
            return()
        hdffile = HDFmain_Bre.main([self.hdf_script_dir, self.ret_dir+'/',
                                    hdfdir,location,instrument,gas,
                                    start_date,end_date,quality])
        return(hdffile)

    def submit_hdf(self, hdffile, subdir=''):
        if self.active == -1:
            print('Set active retrieval first')
            return
        if not self.direc[self.active][4]:
            print('No submission required')
        if subdir == '':
            subdir=self.gas
        try:
            filename = os.path.join(self.hdf_dir, subdir, hdffile)
            h4 = SD.SD(filename)
            quality =  h4.attributes()['DATA_QUALITY']
            if h4.attributes()['FILE_PROJECT_ID'].split(';').count('CAMS27') and quality == 'RD':
                target = 'CAMS27'
            elif quality == 'FINAL':
                target = 'NDACC'
            if target == 'NDACC' or target == 'CAMS27':
                ftp = ftplib.FTP('ftp.hq.ncep.noaa.gov')
                print ('FTP port open')
                ftp.login(passwd='mathias.palm@uni-bremen.de')
                ftp.cwd('/pub/incoming/ndacc')
                print ('Directory changed')
                ftp.storbinary('STOR ' + hdffile, open(filename, 'rb'))
                print ('submission finished')
                ftp.close()
                print ('HDF file submitted')
            else:
                print('Target {} not supported'.format(target))
        except Exception as e:
            print ('HDF file submission failed')
            print(e)
            pass
            

        
        
if __name__ == '__main__':

    import sys, os, getopt
    sys.path.append('/home/mathias/sfit-processing-environment/ModLib')
    sys.path.append('/home/mathias/sfit-processing-environment/Lib_MP')
    sys.path.append('/home/mathias/sfit-processing-environment/HDFsave')


    try:
        opts,arg = getopt.getopt(sys.argv[1:], [], ["init_file=","retrieve=","quality="])
    except:
        print('Call as run_auto_ndacc.py')
        print('error in arguments')
        exit()

    for opt,arg in opts:
        if opt == '--init_file':
            initfile = arg
        if opt == '--retrieve':
            retrieve = arg
        if opt == '--quality':
            quality = arg
            
    rd = run_auto_ndacc(initfile)

    for nr in range(0,len(rd.direcs)):
        print ('Retrieval #%d'%nr)
        rd.set_active(nr)
        rd.retrieve()
        rd.create_tmph5()
        hdffile = rd.create_hdf()
        rd.submit(hdffile)

                    
