package org.scada_lts.cache;

import com.serotonin.mango.vo.DataPointVO;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;
import org.scada_lts.config.ScadaConfig;
import org.scada_lts.dao.DataPointDAO;
import org.scada_lts.quartz.UpdateDataSourcesPoints;

import java.io.IOException;
import java.util.*;

public class DataPointsCache implements IDataPointsCacheWhenStart {

	private static final Log LOG = LogFactory.getLog(DataPointsCache.class);
	
	private boolean cacheEnabled = false;
	
	private static DataPointsCache instance = null;
	
	private Map<Long, List<DataPointVO>> dss = new HashMap<Long, List<DataPointVO>>();
	
	private DataPointsCache() {
		
	}
	
	public static DataPointsCache getInstance() {
		if (instance==null) {
			instance = new DataPointsCache();
		}
		return instance;
	}

	@Override
	public List<DataPointVO> getDataPoints(Long dataSourceId) {
		if (cacheEnabled) {
			return dss.get(dataSourceId);
		} else {
			throw new RuntimeException("Cache may work only when scada start");
		}
	}

	@Override
	public void cacheFinalized() {
		try {
			if (ScadaConfig.getInstance().getBoolean(ScadaConfig.USE_CACHE_DATA_SOURCES_POINTS_WHEN_THE_SYSTEM_IS_READY, false)) {
				cronInitialize();
				cacheEnabled = true;
			} else {
				cacheEnabled = false;
				instance = null;
			}
		} catch (IOException e) {
			LOG.error(e);
		} catch (SchedulerException se) {
			LOG.error(se);
		}
	}

	public void setData(Map<Long, List<DataPointVO>> dss) {
		this.dss = dss;
	}

	@Override
	public void cronInitialize() throws IOException, SchedulerException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("cacheInitialize");
		}
		JobDetail job = new JobDetail();
		job.setName("UpdateDataSourcesPoints");
		job.setJobClass(UpdateDataSourcesPoints.class);

		SimpleTrigger trigger = new SimpleTrigger();
		Date startTime = new Date(System.currentTimeMillis()
				+ ScadaConfig.getInstance().getLong(ScadaConfig.CRONE_UPDATE_CACHE_DATA_SOURCES_POINTS, 10_000_000));
		if (LOG.isTraceEnabled()) {
			LOG.trace("Quartz - startTime:" + startTime);
		}
		trigger.setStartTime(startTime);
		trigger.setRepeatCount(SimpleTrigger.REPEAT_INDEFINITELY);
		Long interval = 5_000_000L;
		if (LOG.isTraceEnabled()) {
			LOG.trace("Quartz - interval:" + interval);
		}
		trigger.setRepeatInterval(interval);
		trigger.setName("Quartz - trigger-UpdateDataSourcesPoints");

		Scheduler scheduler = new StdSchedulerFactory().getScheduler();
		scheduler.start();
		scheduler.scheduleJob(job, trigger);
	}

	public boolean isCacheEnabled() {
		return cacheEnabled;
	}

	@Override
	public void cacheInitialize() {
		
		List<DataPointVO> dps = new DataPointDAO().getDataPoints();
		
		dss = composeCashData(dps);
		
		cacheEnabled = true;
	}
	
	public Map<Long, List<DataPointVO>> composeCashData(List<DataPointVO> dps) {
		
		Map<Long, List<DataPointVO>> dss = new HashMap<Long, List<DataPointVO>>();
		if (dps != null && dps.size()>0) {
			for (DataPointVO dp : dps) {
				List<DataPointVO> cacheDs = dss.get((long)dp.getDataSourceId()); 
				if (cacheDs==null) {
					cacheDs = new ArrayList<DataPointVO>();
					cacheDs.add(dp);
					dss.put((long) dp.getDataSourceId(), cacheDs);			
				} else {
					cacheDs.add(dp);
				}
			}
		}
		
		return dss;
		
		
	}

}
