package main

import (
	"fmt"
	"sync"
	"os"
	"time"
	"loadconfig"
	"strings"
	"runtime/trace"
)

var logger = NewLogger()

var (
	g_bufsize = 1 * 1024 * 1024 // M default
	g_configfile string
	g_sys_configfile string = "sys.yml"
	g_readernum = 1 // default
	g_nodenum = 0
	g_tablenum = 0
	g_maxtuplechunk int64 = 0
	g_dbinfos []DBInfo
	g_tableinfos []TableInfo
	g_quiet = false
	g_filesize int64 = 0
	g_jobs []*Job
	g_slice_num = 0
	g_encoding string
	g_has_csv_header bool = false
	jwg sync.WaitGroup
)

func loadConfig(configFile string) (*loadconfig.Config) {
	if configFile == "" {
		logger.Error("configuration file not provided ..")
		os.Exit(1)
	}
	
	conf, err := loadconfig.ReadConfigData(configFile)
	if err != nil {
		logger.Fatal(err.Error())
	}
	return conf
}

func loadSysConfig(configFile string) (*loadconfig.SysConfig) {
	if configFile == "" {
		logger.Error("system configuration file not provided ..")
		os.Exit(1)
	}
	
	conf, err := loadconfig.ReadSysConfigData(configFile)
	if err != nil {
		logger.Warn("no system configuration provided")
		return nil
	}
	return conf
}


func sysinit(conf *loadconfig.Config, sysconf *loadconfig.SysConfig) {
	logger.set_log_level(conf.Loglevel)

	g_readernum = conf.Readers
	g_nodenum = len(conf.Nodes)
	g_tablenum = len(conf.Tables)
	g_maxtuplechunk = conf.Maxtuplechunk
	g_slice_num = conf.Slicenum
	g_encoding = conf.Encoding
	g_has_csv_header = conf.Csvheader

	if sysconf != nil {
		g_BasketTupleSize = sysconf.Basket_tuple_size * 1024 * 1024
		g_DataQueueSize = sysconf.Max_data_queue_sync_size
		g_bufsize = sysconf.Io_read_size * 1024 * 1024
	}

	g_tableinfos = make([]TableInfo, 0)
	for i:=0; i<g_tablenum; i++ {
		t := conf.Tables[i]
		g_tableinfos = append(g_tableinfos,
			TableInfo{
				name: t.Tablename,
				columns:strings.Split(t.Columns, ","),
				datapath: t.Datapath,
				partitionField: t.PartitionField,
				partitionFieldType: t.PartitionFieldType,
				schema: conf.Schema,
			})
	}

	if g_slice_num != g_nodenum {
		logger.Error("configuration not consistant: slice nubmer should be equal to g_nodenum")
		os.Exit(1)
	}
	
	g_dbinfos = make([]DBInfo, g_slice_num)

	for i:=0; i < g_slice_num; i++ {
		remainder := i;

		// target_node_index := remainder % g_nodenum
		// n := conf.Nodes[target_node_index]
		n := conf.Nodes[remainder]
		g_dbinfos[i] = DBInfo{
			host: n.Host,
			port: n.Port,
			remainder: remainder,
			user: conf.User,
			password: conf.Password,
			dbname: conf.Dbname,
			schema: conf.Schema,
		}
	}
}


func showConfigInfo() {
	var info = "\n-----Distributed Database Data Loading Tool-----\n"
	info += fmt.Sprintf("  node number:\t%d\n", g_nodenum)
	info += fmt.Sprintf("  slice number:\t%d\n", g_slice_num)
	info += fmt.Sprintf("  encoding:\t%s\n", g_encoding)
	info += fmt.Sprintf("   csv header:\t%t\n", g_has_csv_header)
	for i:=0; i<g_slice_num; i++ {
		d := g_dbinfos[i]
		info += fmt.Sprintf("    remainder: %d, host: %s, port: %d, user: %s, db: %s\n",
			d.remainder, d.host, d.port, d.user, d.dbname)
	}

	info += fmt.Sprintf("  reader numbber: %d\n", g_readernum)

	info += "Tables:\n"
	for i, c := range g_tableinfos {
		info += fmt.Sprintf("  [%d] table name: %s\n", i, c.name)
		info += fmt.Sprintf("       columns: %s\n", strings.Join(c.columns, ","))
		info += fmt.Sprintf("       schema: %s\n", c.schema)
		info += fmt.Sprintf("       datapath: %s\n", c.datapath)
		info += fmt.Sprintf("       partitionFIeld: %d %s\n", c.partitionField, c.partitionFieldType)
	}

	info += "System Parameters:\n"
	buflen, size := sizeConvert(int64(g_bufsize))
	info += fmt.Sprintf("  IOreadSize:\t%d(%d%s)\n", g_bufsize, buflen, size)
	buflen, size = sizeConvert(int64(g_BasketTupleSize))
	info += fmt.Sprintf("  BasketTupleSize:\t%d(%d%s)\n", g_BasketTupleSize, buflen, size)
	info += fmt.Sprintf("  DataQueueSize:\t%d\n", g_DataQueueSize)
	
	info += "\n------ end of configuration ------\n"
	
	fmt.Println(info)
}


func prepareJobs() {
	logger.Debug("prepare jobs start...")
	g_jobs = make([]*Job, 0)

	for i:=0; i<g_tablenum; i++ {
		job := NewJob(i, &g_tableinfos[i], &jwg)
		g_jobs = append(g_jobs, job)
	}
	logger.Debug("there totally %d jobs to be processed", len(g_jobs))
	logger.Debug("prepare jobs end..")
}

func validateJobs() {
	logger.Debug("validate jobs start...")
	for i, job := range g_jobs {
		logger.Info("[%d] job start to validation...", i)
		job.validate()
		logger.Info("[%d] job validation successfully complete", i)
	}
	logger.Debug("validate jobs end...")
}

func processJobs() {
	logger.Debug("process jobs start...")
	for i, job := range g_jobs {
		jwg.Add(1)
		logger.Debug("[%d] job start to process...", i)
		go job.process()
	}
	jwg.Wait()
	logger.Debug("process jobs end...")
}

func endJobs() {
	logger.Debug("end jobs start...")
	//var total_handlecount int64 = 0
	//for i, r := range readerlist {
	//	logger.Printf("reader[%d] handlecount: %d\n", i, r.handlecount)
	//	total_handlecount += r.handlecount
	//	logger.Printf("basket count %d\n", r.basketcount)
	//}
	//logger.Printf("total handle count is %d\n", total_handlecount)
	//logger.Println("total execution interval is", time.Since(start))
	logger.Debug("end jobs end...")
}


func main() {
        ft, err := os.Create("trace.out")
        defer ft.Close()

        err = trace.Start(ft)
        defer trace.Stop()
        if err != nil {
            logger.Info("----fail to trace\n")
           panic(err) 
        }

	if len(os.Args) == 2 {
		g_configfile = os.Args[1]
	} else if (len(os.Args) == 3) {
		g_configfile = os.Args[1]
		if os.Args[2] == "-q" {
			g_quiet = true
		}
	}

	conf := loadConfig(g_configfile)
	sysconf := loadSysConfig(g_sys_configfile)

	sysinit(conf, sysconf)
	if !g_quiet {
		showConfigInfo()
	}

	if !g_quiet {
		fmt.Println("press enter to continue...")
		fmt.Scanln()
	}
	logger.Info("work start ...")
	
	start := time.Now()

	prepareJobs()
	validateJobs()
	processJobs()
	endJobs()

	logger.Info("total execution interval is %s", time.Since(start))
	logger.Info("all work done")
}
