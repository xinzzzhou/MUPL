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
package net.librec.data;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Table;
import net.librec.math.structure.SparseMatrix;
import net.librec.math.structure.SparseTensor;

/**
 * A <tt>DataConvertor</tt> is an interface to convert
 * a data file from one source format to a target format.
 *
 * @author WangYuFeng
 */
public interface DataConvertor {

    /**
     * Process the input data.
     *
     * @throws IOException
     *         if the path is not valid
     */
    void processData() throws IOException;

    /**
     * author:zhouxin
     * @throws IOException
     */
    void processDatas() throws IOException;
    /**
     * Returns a {@code SparseMatrix} object which stores rate data.
     *
     * @return a {@code SparseMatrix} object which stores rate data.
     */
    SparseMatrix getPreferenceMatrix();

    /**
     * Returns a {@code SparseMatrix} object which stores time data.
     *
     * @return a {@code SparseMatrix} object which stores time data.
     */
    SparseMatrix getDatetimeMatrix();

    /**
     * Returns a {@code SparseTensor} object which stores rate data.
     *
     * @return a {@code SparseTensor} object which stores rate data.
     */
    SparseTensor getSparseTensor();

    Table<Integer, Integer, Integer> getAction(int action);
    Map<Integer, Integer> getCount(int action);
}
