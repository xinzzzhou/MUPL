/**
 * Copyright (C) 2016 LibRec
 * <p>
 * This file is part of LibRec.
 * LibRec is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * LibRec is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with LibRec. If not, see <http://www.gnu.org/licenses/>.
 */
package net.librec.data.model;

import com.google.common.collect.Table;
import net.librec.common.LibrecException;
import net.librec.conf.Configured;
import net.librec.data.*;
import net.librec.data.splitter.KCVDataSplitter;
import net.librec.math.structure.DataSet;
import net.librec.math.structure.SparseMatrix;
import net.librec.util.DriverClassUtil;
import net.librec.util.ReflectionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

/**
 * A <tt>AbstractDataModel</tt> represents a data access class to the input
 * file.
 *
 * @author WangYuFeng
 */
public abstract class AbstractDataModel extends Configured implements DataModel {
    /**
     * LOG
     */
    protected final Log LOG = LogFactory.getLog(this.getClass());
    /**
     * context
     */
    protected DataContext context;
    /**
     * train DataSet
     */
    protected DataSet trainDataSet;
    /**
     * test DataSet
     */
    protected DataSet testDataSet;
    /**
     * valid DataSet
     */
    protected DataSet validDataSet;

    /** The convertor of the model {@link net.librec.data.DataConvertor} */
    protected DataConvertor dataConvertor;

    /**
     * Data Splitter {@link net.librec.data.DataSplitter}
     */
    public DataSplitter dataSplitter;
    /**
     * Data Splitter {@link DataAppender}
     */
    public DataAppender dataAppender;

    double[][] userAction;
    protected SparseMatrix trainMatrix;
    /**
     * Build Convert.
     *
     * @throws LibrecException
     *             if error occurs when building convert.
     */
    protected abstract void buildConvert() throws LibrecException;

    /**
     * author: zhouxin
     * @throws LibrecException
     */
    protected abstract void buildConverts() throws LibrecException;

    /**
     * Build Splitter.
     *
     * @throws LibrecException
     *             if error occurs when building splitter.
     */
    protected void buildSplitter() throws LibrecException {
        String splitter = conf.get("data.model.splitter");
        try {
        	if (dataSplitter == null){
        		dataSplitter = (DataSplitter) ReflectionUtil.newInstance(DriverClassUtil.getClass(splitter), conf);	
        	}
            if (dataSplitter != null) {
                dataSplitter.setDataConvertor(dataConvertor);
                if (dataSplitter instanceof KCVDataSplitter) {
                    ((KCVDataSplitter) dataSplitter).splitFolds();
                }
                dataSplitter.splitData();
                trainDataSet = dataSplitter.getTrainData();
                testDataSet = dataSplitter.getTestData();
            }
        } catch (ClassNotFoundException e) {
            throw new LibrecException(e);
        }
    }

    /**
     * author:zhouxin
     * @throws LibrecException
     */
    protected void buildSplitters() throws LibrecException {
        String splitter = conf.get("data.model.splitter");
        try {
            if (dataSplitter == null){
                dataSplitter = (DataSplitter) ReflectionUtil.newInstance(DriverClassUtil.getClass(splitter), conf);
            }
            if (dataSplitter != null) {
                dataSplitter.setDataConvertor(dataConvertor);
                if (dataSplitter instanceof KCVDataSplitter) {
                    ((KCVDataSplitter) dataSplitter).splitFolds();
                }
                dataSplitter.splitData();
                trainDataSet = dataSplitter.getTrainData();
                testDataSet = dataSplitter.getTestData();
//                trainMatrix = (SparseMatrix)trainDataSet;

//                int numUser = trainMatrix.numRows;
//                int numAction = conf.getInt("rec.num.action");
//                userAction = new double[numUser][numAction];
//                initAction(numUser, numAction);
//                computeAction(numUser, numAction);
            }
        } catch (ClassNotFoundException e) {
            throw new LibrecException(e);
        }
    }

    /**
     * Build appender data.
     * 
     * @throws LibrecException
     *             if error occurs when building appender.
     */
    protected void buildFeature() throws LibrecException {
        String feature = conf.get("data.appender.class");
        if (StringUtils.isNotBlank(feature)) {
            try {
                dataAppender = (DataAppender) ReflectionUtil.newInstance(DriverClassUtil.getClass(feature), conf);
                dataAppender.setUserMappingData(getUserMappingData());
                dataAppender.setItemMappingData(getItemMappingData());
                dataAppender.processData();
            } catch (ClassNotFoundException e) {
                throw new LibrecException(e);
            } catch (IOException e) {
                throw new LibrecException(e);
            }
        }
    }

    /**
     * Build data model.
     *
     * @throws LibrecException
     *             if error occurs when building model.
     */
    @Override
    public void buildDataModel() throws LibrecException {
        context = new DataContext(conf);
        if (!conf.getBoolean("data.convert.read.ready")) {
            if (conf.getBoolean("rec.recommender.isOriginal",true)){
                buildConvert();
            }else {
                buildConverts();
            }
            LOG.info("Transform data to Convertor successfully!");
//            conf.setBoolean("data.convert.read.ready", true);
        }

        if (conf.getBoolean("rec.recommender.isOriginal",true)) {
            buildSplitter();
        }else {
            buildSplitters();
        }

        LOG.info("Split data to train Set and test Set successfully!");
        if (trainDataSet != null && trainDataSet.size() > 0 && testDataSet != null && testDataSet.size() > 0) {
            LOG.info("Data size of training is " + trainDataSet.size());
            LOG.info("Data size of testing is " + testDataSet.size());
        }
        if (StringUtils.isNotBlank(conf.get("data.appender.class")) && !conf.getBoolean("data.appender.read.ready")) {
            buildFeature();
            LOG.info("Transform data to Feature successfully!");
            conf.setBoolean("data.appender.read.ready", true);
        }
    }

    /**
     * Load data model.
     *
     * @throws LibrecException
     *             if error occurs during loading
     */
    @Override
    public void loadDataModel() throws LibrecException {
        // TODO Auto-generated method stub

    }

    /**
     * Save data model.
     *
     * @throws LibrecException
     *             if error occurs during saving
     */
    @Override
    public void saveDataModel() throws LibrecException {
        // TODO Auto-generated method stub

    }

    /**
     * Get train data set.
     *
     * @return the train data set of data model.
     */
    @Override
    public DataSet getTrainDataSet() {
        return trainDataSet;
    }

    /**
     * Get test data set.
     *
     * @return the test data set of data model.
     */
    @Override
    public DataSet getTestDataSet() {
        return testDataSet;
    }

    /**
     * Get valid data set.
     *
     * @return the valid data set of data model.
     */
    @Override
    public DataSet getValidDataSet() {
        return validDataSet;
    }

    /**
     * Get data splitter.
     *
     * @return the splitter of data model.
     */
    @Override
    public DataSplitter getDataSplitter() {
        return dataSplitter;
    }

    /**
     * Get data appender.
     *
     * @return the appender of data model.
     */
    public DataAppender getDataAppender() {
        return dataAppender;
    }

    /**
     * Get data context.
     *
     * @return the context see {@link net.librec.data.DataContext}.
     */
    @Override
    public DataContext getContext() {
        return context;
    }

//    public void computeAction(int user, int action){
//
//        for (int userIdx = 0; userIdx < user; userIdx++){
//            //item list to iterate
//            List<Integer> itemList = trainMatrix.getColumnsImpFeedback(userIdx);
//            for (int itemIdx = 0; itemIdx < itemList.size(); itemIdx++){
//                int[] uiAction = getActions(userIdx, itemList.get(itemIdx));
//
//                for (int actionIdx = 0; actionIdx < action; actionIdx++){
//                    userAction[userIdx][actionIdx] += uiAction[actionIdx];
//                }
//            }
//        }
//    }
//
//    public int[] getActions(int userIdx, int itemIdx){
//        int[] actions = new int[]{0,0,0,0,0,0,0,0,0,0};
//
//        Table<Integer, Integer, Integer> action1Table;
//        Table<Integer, Integer, Integer> action2Table;
//        Table<Integer, Integer, Integer> action3Table;
//        Table<Integer, Integer, Integer> action4Table;
//        Table<Integer, Integer, Integer> action5Table;
//        Table<Integer, Integer, Integer> action6Table;
//        Table<Integer, Integer, Integer> action7Table;
//        Table<Integer, Integer, Integer> action8Table;
//        Table<Integer, Integer, Integer> action9Table;
//
//        action1Table = dataConvertor.getAction(1);
//        action2Table = dataConvertor.getAction(2);
//        action3Table = dataConvertor.getAction(3);
//        action4Table = dataConvertor.getAction(4);
//        action5Table = dataConvertor.getAction(5);
//        action6Table = dataConvertor.getAction(6);
//        action7Table = dataConvertor.getAction(7);
//        action8Table = dataConvertor.getAction(8);
//        action9Table = dataSplitter.getAction9Table();
//
//        if (action1Table.contains(userIdx, itemIdx)){
//            actions[0] = action1Table.get(userIdx, itemIdx);
//        }
//        if (action2Table.contains(userIdx, itemIdx)){
//            actions[1] = action2Table.get(userIdx, itemIdx);
//        }
//        if (action3Table.contains(userIdx, itemIdx)){
//            actions[2] = action3Table.get(userIdx, itemIdx);
//        }
//        if (action4Table.contains(userIdx, itemIdx)){
//            actions[3] = action4Table.get(userIdx, itemIdx);
//        }
//        if (action5Table.contains(userIdx, itemIdx)){
//            actions[4] = action5Table.get(userIdx, itemIdx);
//        }
//        if (action6Table.contains(userIdx, itemIdx)){
//            actions[5] = action6Table.get(userIdx, itemIdx);
//        }
//        if (action7Table.contains(userIdx, itemIdx)){
//            actions[6] = action7Table.get(userIdx, itemIdx);
//        }
//        if (action8Table.contains(userIdx, itemIdx)){
//            actions[7] = action8Table.get(userIdx, itemIdx);
//        }
//        if (action9Table.contains(userIdx, itemIdx)){
//            actions[8] = action9Table.get(userIdx, itemIdx);
//        }
//        return actions;
//    }
//
//    public void initAction(int user, int item){
//        for (int uIdx = 0; uIdx < user; uIdx ++){
//            for (int iIdx = 0; iIdx < item; iIdx ++){
//                userAction[uIdx][iIdx] = 0.0;
//            }
//        }
//    }
}
