/**
 * Copyright (C) 2016 LibRec
 *
 * This file is part of LibRec.
 * LibRec is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LibRec is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LibRec. If not, see <http://www.gnu.org/licenses/>.
 */
package net.librec.recommender.cf.ranking;

import net.librec.BaseTestCase;
import net.librec.common.LibrecException;
import net.librec.conf.Configuration;
import net.librec.job.RecommenderJob;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * BPRPlus Test Case corresponds to BPRRecommender
 *
 *
 */
public class BPRPlusTestCase extends BaseTestCase {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }
    /**
     * test the whole process of BPRPlus recommendation
     *
     * @throws ClassNotFoundException
     * @throws LibrecException
     * @throws IOException
     */
    @Test
    public void testRecommender() throws ClassNotFoundException, LibrecException, IOException {
        String path = "rec/cf/ranking/bprplus-test1-0.properties";
        Configuration.Resource resource = new Configuration.Resource(path);
        conf.addResource(resource);
        RecommenderJob job = new RecommenderJob(conf);
        job.runJob();

//        for (int i = 1; i <= 10; i++){
//            path = "rec/cf/ranking/bprplus-test1-"+i+".properties";
//            resource = new Configuration.Resource(path);
//            conf.addResource(resource);
//            job = new RecommenderJob(conf);
//            job.runJob();
//        }
//
//        path = "rec/cf/ranking/bprplus-test1-20.properties";
//        resource = new Configuration.Resource(path);
//        conf.addResource(resource);
//        job = new RecommenderJob(conf);
//        job.runJob();
//        path = "rec/cf/ranking/bprplus-test2.properties";
//        resource = new Configuration.Resource(path);
//        conf.addResource(resource);
//        job = new RecommenderJob(conf);
//        job.runJob();
////
//        path = "rec/cf/ranking/bprplus-test3.properties";
//        resource = new Configuration.Resource(path);
//        conf.addResource(resource);
//        job = new RecommenderJob(conf);
//        job.runJob();
    }
}
