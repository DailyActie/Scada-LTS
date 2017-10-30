/*
 * (c) 2017 Abil'I.T. http://abilit.eu/
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.scada_lts.quartz;

import com.serotonin.mango.vo.DataPointVO;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;
import org.scada_lts.cache.DataPointsCache;
import org.scada_lts.dao.DataPointDAO;

import java.util.List;
import java.util.Map;

/**
 * Update data job for data sources points in cache.
 *
 * @author grzegorz bylica Abil'I.T. development team, grzegorz.bylica@gmail.com
 *
 */
public class UpdateDataSourcesPoints implements StatefulJob {

    private static final Log LOG = LogFactory.getLog(UpdateDataSourcesPoints.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        LOG.trace("UpdateEventDetectors");
        List<DataPointVO> dps = new DataPointDAO().getDataPoints();
        Map<Long, List<DataPointVO>> dss = DataPointsCache.getInstance().composeCashData(dps);
        DataPointsCache.getInstance().setData(dss);

    }
}
