package org.scada_lts.cache;

import com.serotonin.mango.vo.DataPointVO;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.util.List;

public interface IDataPointsCacheWhenStart {
	
	List<DataPointVO> getDataPoints(Long dataSourceId);
	
	void cacheInitialize();
	 
	void cacheFinalized();

	void cronInitialize() throws IOException, SchedulerException;
	
	
	
}
