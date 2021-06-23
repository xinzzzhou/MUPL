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

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.BiMap;

import com.google.common.collect.Table;
import net.librec.common.LibrecException;
import net.librec.conf.Configuration;
import net.librec.conf.Configured;
import net.librec.data.DataModel;
import net.librec.data.convertor.TextDataConvertor;
import net.librec.math.structure.DataSet;
import org.apache.commons.lang.StringUtils;

/**
 * A <tt>TextDataModel</tt> represents a data access class to the CSV format
 * input.
 *
 * @author WangYuFeng
 */
public class TextDataModel extends AbstractDataModel implements DataModel {

    /**
     * Empty constructor.
     */
    public TextDataModel() {
    }

    /**
     * Initializes a newly created {@code TextDataModel} object with
     * configuration.
     *
     * @param conf
     *            the configuration for the model.
     */
    public TextDataModel(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Build Convert.
     *
     * @throws LibrecException
     *             if error occurs during building
     */
    @Override
    public void buildConvert() throws LibrecException {
//        String inputDataPath = conf.get(Configured.CONF_DFS_DATA_DIR) + "/" + conf.get(Configured.CONF_DATA_INPUT_PATH);
        String[] inputDataPath = conf.get(Configured.CONF_DATA_INPUT_PATH).trim().split(" ");
        for(int i = 0 ; i < inputDataPath.length; i ++){
            inputDataPath[i]=conf.get(Configured.CONF_DFS_DATA_DIR)+"/"+inputDataPath[i];
        }
        String dataColumnFormat = conf.get(Configured.CONF_DATA_COLUMN_FORMAT, "UIR");
        dataConvertor = new TextDataConvertor(dataColumnFormat, StringUtils.join(inputDataPath," "), conf.getDouble("data.convert.binarize.threshold", -1.0));
        try {
            dataConvertor.processData();
        } catch (IOException e) {
            throw new LibrecException(e);
        }
    }

    /**
     * author:zhouxin
     * @throws LibrecException
     */
    @Override
    protected void buildConverts() throws LibrecException {
        String[] inputDataPath = conf.get(Configured.CONF_DATA_INPUT_PATH).trim().split(" ");
        for(int i = 0 ; i < inputDataPath.length; i ++){
            inputDataPath[i]=conf.get(Configured.CONF_DFS_DATA_DIR)+"/"+inputDataPath[i];
        }
        String dataColumnFormat = conf.get(Configured.CONF_DATA_COLUMN_FORMAT, "UIR");
        dataConvertor = new TextDataConvertor(dataColumnFormat, StringUtils.join(inputDataPath," "), conf.getDouble("data.convert.binarize.threshold", -1.0));
        try {
            dataConvertor.processDatas();
        } catch (IOException e) {
            throw new LibrecException(e);
        }
    }

    /**
     * Load data model.
     *
     */
    @Override
    public void loadDataModel() {

    }

    /**
     * Save data model.
     *
     */
    @Override
    public void saveDataModel() {

    }

    /**
     * Get user mapping data.
     *
     * @return the user {raw id, inner id} map of data model.
     */
    @Override
    public BiMap<String, Integer> getUserMappingData() {
        return ((TextDataConvertor) dataConvertor).getUserIds();
    }

    /**
     * Get item mapping data.
     *
     * @return the item {raw id, inner id} map of data model.
     */
    @Override
    public BiMap<String, Integer> getItemMappingData() {
        return ((TextDataConvertor) dataConvertor).getItemIds();
    }

    @Override
    public Table<Integer, Integer, Integer> getActionTable(int level) {
        switch (level){
            case 1:
                return dataConvertor.getAction(1);
            case 2:
                return dataConvertor.getAction(2);
            case 3:
                return dataConvertor.getAction(3);
            case 4:
                return dataConvertor.getAction(4);
            case 5:
                return dataConvertor.getAction(5);
            case 6:
                return dataConvertor.getAction(6);
            case 7:
                return dataConvertor.getAction(7);
            case 8:
                return dataConvertor.getAction(8);
            case 9:
                return dataSplitter.getAction9Table();
        }
        return null;
    }

    @Override
    public Map<Integer, Integer> getItemCount(int action) {
        if (action == 9){
            return dataSplitter.getAction9Count();
        }else {
            return dataConvertor.getCount(action);
        }
    }

    @Override
    public double[][] getUserAction() {
        return userAction;
    }

    /**
     * Get datetime data set.
     *
     * @return the datetime data set of data model.
     */
    @Override
    public DataSet getDatetimeDataSet() {
        return dataConvertor.getDatetimeMatrix();
    }
}
