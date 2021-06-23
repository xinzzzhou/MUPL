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
package net.librec.recommender;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table;
import net.librec.common.LibrecException;
import net.librec.conf.Configuration;
import net.librec.data.DataModel;
import net.librec.eval.Measure;
import net.librec.eval.Measure.MeasureValue;
import net.librec.eval.RecommenderEvaluator;
import net.librec.math.algorithm.Maths;
import net.librec.math.structure.DenseMatrix;
import net.librec.math.structure.MatrixEntry;
import net.librec.math.structure.SparseMatrix;
import net.librec.recommender.item.*;
import net.librec.util.ReflectionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Abstract Recommender Methods
 *
 * @author WangYuFeng and Wang Keqiang
 */
public abstract class AbstractRecommender implements Recommender {
    /**
     * LOG
     */
    protected final Log LOG = LogFactory.getLog(this.getClass());

    /**
     * is ranking or rating
     */
    protected boolean isRanking;

    /**
     * topN
     */
    protected int topN;

    /**
     * conf
     */
    protected Configuration conf;

    /**
     * RecommenderContext
     */
    protected RecommenderContext context;

    /**
     * trainMatrix
     */
    protected SparseMatrix trainMatrix;

    /**
     * testMatrix
     */
    protected SparseMatrix testMatrix;

    /**
     * validMatrix
     */
    protected SparseMatrix validMatrix;

    /**
     * Recommended Item List
     */
    protected RecommendedList recommendedList;

    /**
     * the number of users
     */
    protected int numUsers;

    /**
     * the number of items
     */
    protected int numItems;

    /**
     * the number of rates
     */
    protected int numRates;

    /**
     * Maximum rate of rating scale
     */
    protected double maxRate;

    /**
     * Minimum rate of rating scale
     */
    protected double minRate;

    /**
     * a list of rating scales
     */
    protected static List<Double> ratingScale;

    /**
     * user Mapping Data
     */
    public BiMap<String, Integer> userMappingData;

    /**
     * item Mapping Data
     */
    public BiMap<String, Integer> itemMappingData;

    /**
     * global mean of ratings
     */
    protected double globalMean;

    /**
     * early-stop criteria
     */
    protected boolean earlyStop;

    /**
     * verbose
     */
    protected static boolean verbose = true;

    /**
     * objective loss
     */
    protected double loss, lastLoss = 0.0d;

    /**
     * whether to adjust learning rate automatically
     */
    protected boolean isBoldDriver;

    /**
     * decay of learning rate
     */
    protected float decay;

    /**
     * Table<userIdx, itemIdx, action>
     */
    protected Table<Integer, Integer, Integer> a1Action;
    protected Table<Integer, Integer, Integer> a2Action;
    protected Table<Integer, Integer, Integer> a3Action;
    protected Table<Integer, Integer, Integer> a4Action;
    protected Table<Integer, Integer, Integer> a5Action;
    protected Table<Integer, Integer, Integer> a6Action;
    protected Table<Integer, Integer, Integer> a7Action;
    protected Table<Integer, Integer, Integer> a8Action;
    protected Table<Integer, Integer, Integer> a9Action;

    protected Map<Integer, Double> itemPops;

    protected int[] actionCount;

    protected double avgCount;

    protected int totalCount;

    protected int numActions;

    protected double[][] implicitWeight;

    /**
     * setup
     *
     * @throws LibrecException if error occurs during setup
     */
    protected void setup() throws LibrecException {
        conf = context.getConf();
        isRanking = conf.getBoolean("rec.recommender.isranking");
        if (isRanking) {
            topN = conf.getInt("rec.recommender.ranking.topn", 10);
            if (this.topN <= 0) {
                throw new IndexOutOfBoundsException("rec.recommender.ranking.topn should be more than 0!");
            }
        }
        earlyStop = conf.getBoolean("rec.recommender.earlystop", false);
        verbose = conf.getBoolean("rec.recommender.verbose", true);

        trainMatrix = (SparseMatrix) getDataModel().getTrainDataSet();
        testMatrix = (SparseMatrix) getDataModel().getTestDataSet();
        validMatrix = (SparseMatrix) getDataModel().getValidDataSet();
        userMappingData = getDataModel().getUserMappingData();
        itemMappingData = getDataModel().getItemMappingData();

//        a1Action = getDataModel().getActionTable(1);
//        a2Action = getDataModel().getActionTable(2);
//        a3Action = getDataModel().getActionTable(3);
//        a4Action = getDataModel().getActionTable(4);
//        a5Action = getDataModel().getActionTable(5);
//        a6Action = getDataModel().getActionTable(6);
//        a7Action = getDataModel().getActionTable(7);
//        a8Action = getDataModel().getActionTable(8);
//        a9Action = getDataModel().getActionTable(9);

        numUsers = trainMatrix.numRows();
        numItems = trainMatrix.numColumns();
        numRates = trainMatrix.size();
        numActions = Integer.parseInt(conf.get("rec.num.action"));

        implicitWeight = new double[numUsers][numActions];

        ratingScale = new ArrayList<>(trainMatrix.getValueSet());
        Collections.sort(ratingScale);
        maxRate = Collections.max(trainMatrix.getValueSet());
        minRate = Collections.min(trainMatrix.getValueSet());
        if (minRate == maxRate) {
            minRate = 0;
        }
        globalMean = trainMatrix.mean();

        int[] numDroppedItemsArray = new int[numUsers]; // for AUCEvaluator
        int maxNumTestItemsByUser = 0; //for idcg
        for (int userIdx = 0; userIdx < numUsers; ++userIdx) {
            numDroppedItemsArray[userIdx] = numItems - trainMatrix.rowSize(userIdx);
            int numTestItemsByUser = testMatrix.rowSize(userIdx);
            maxNumTestItemsByUser = maxNumTestItemsByUser < numTestItemsByUser ? numTestItemsByUser : maxNumTestItemsByUser;
        }
        conf.setInts("rec.eval.auc.dropped.num", numDroppedItemsArray);
        conf.setInt("rec.eval.item.test.maxnum", maxNumTestItemsByUser);
    }

    /**
     * train Model
     *
     */
    protected abstract void trainModel() throws Exception;

    /**
     * recommend
     *
     * @param context recommender context
     * @throws LibrecException if error occurs during recommending
     */
    @Override
    public void recommend(RecommenderContext context) throws LibrecException {
        this.context = context;
        setup();
        LOG.info("Job Setup completed.");
        try {
            trainModel();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Job Train completed.");
        this.recommendedList = recommend();
        LOG.info("Job End.");
        cleanup();
    }

    /**
     * recommend
     * * predict the ranking scores or ratings in the test data
     *
     * @return predictive ranking score or rating matrix
     * @throws LibrecException if error occurs during recommending
     */
    protected RecommendedList recommend() throws LibrecException {
        if (isRanking && topN > 0) {
            recommendedList = recommendRank();
        } else {
            recommendedList = recommendRating();
        }
        return recommendedList;
    }

    /**
     * recommend
     * * predict the ranking scores in the test data
     *
     * @return predictive rating matrix
     * @throws LibrecException if error occurs during recommending
     */
    protected RecommendedList recommendRank() throws LibrecException {
        recommendedList = new RecommendedItemList(numUsers - 1, numUsers);

        for (int userIdx = 0; userIdx < numUsers; ++userIdx) {
            Set<Integer> itemSet = trainMatrix.getColumnsSet(userIdx);
            for (int itemIdx = 0; itemIdx < numItems; ++itemIdx) {
                if (itemSet.contains(itemIdx)) {
                    continue;
                }
                double predictRating = predict(userIdx, itemIdx);
                if (Double.isNaN(predictRating)) {
                    continue;
                }
                recommendedList.addUserItemIdx(userIdx, itemIdx, predictRating);
            }
            recommendedList.topNRankItemsByUser(userIdx, topN);
        }

        if (recommendedList.size() == 0) {
            throw new IndexOutOfBoundsException("No item is recommended, there is something error in the recommendation algorithm! Please check it!");
        }

        return recommendedList;
    }

    /**
     * recommend
     * * predict the ratings in the test data
     *
     * @return predictive rating matrix
     * @throws LibrecException if error occurs during recommending
     */
    protected RecommendedList recommendRating() throws LibrecException {
        recommendedList = new RecommendedItemList(numUsers - 1, numUsers);

        for (MatrixEntry matrixEntry : testMatrix) {
            int userIdx = matrixEntry.row();
            int itemIdx = matrixEntry.column();
            double predictRating = predict(userIdx, itemIdx, true);
            if (Double.isNaN(predictRating)) {
                predictRating = globalMean;
            }
            recommendedList.addUserItemIdx(userIdx, itemIdx, predictRating);
        }

        return recommendedList;
    }

    /**
     * predict a specific rating for user userIdx on item itemIdx, note that the
     * prediction is not bounded. It is useful for building models with no need
     * to bound predictions.
     *
     * @param userIdx user index
     * @param itemIdx item index
     * @return predictive rating for user userIdx on item itemIdx without bound
     * @throws LibrecException if error occurs during predicting
     */
    protected abstract double predict(int userIdx, int itemIdx) throws LibrecException;

    /**
     * predict a specific rating for user userIdx on item itemIdx. It is useful for evalution which requires predictions are
     * bounded.
     *
     * @param userIdx user index
     * @param itemIdx item index
     * @param bound   whether there is a bound
     * @return predictive rating for user userIdx on item itemIdx with bound
     * @throws LibrecException if error occurs during predicting
     */
    protected double predict(int userIdx, int itemIdx, boolean bound) throws LibrecException {
        double predictRating = predict(userIdx, itemIdx);

        if (bound) {
            if (predictRating > maxRate) {
                predictRating = maxRate;
            } else if (predictRating < minRate) {
                predictRating = minRate;
            }
        }

        return predictRating;
    }

    /**
     * evaluate
     *
     * @param evaluator recommender evaluator
     */
    public double evaluate(RecommenderEvaluator evaluator) {
        return evaluator.evaluate(context, recommendedList);
    }

    /**
     * evaluate Map
     *
     * @return evaluate map
     */
    public Map<MeasureValue, Double> evaluateMap() {
        Map<MeasureValue, Double> evaluatedMap = new HashMap<>();
        List<MeasureValue> measureValueList = Measure.getMeasureEnumList(isRanking, topN);
        if (measureValueList != null) {
            for (MeasureValue measureValue : measureValueList) {
                RecommenderEvaluator evaluator = ReflectionUtil
                        .newInstance(measureValue.getMeasure().getEvaluatorClass());
                if (isRanking && measureValue.getTopN() != null && measureValue.getTopN() > 0) {
                    evaluator.setTopN(measureValue.getTopN());
                }
                double evaluatedValue = evaluator.evaluate(context, recommendedList);
                evaluatedMap.put(measureValue, evaluatedValue);
            }
        }
        return evaluatedMap;
    }

    /**
     * cleanup
     *
     */
    protected void cleanup() {

    }

    /**
     * (non-Javadoc)
     *
     * @see net.librec.recommender.Recommender#loadModel(String)
     */
    @Override
    public void loadModel(String filePath) {
    }

    /**
     * (non-Javadoc)
     *
     * @see net.librec.recommender.Recommender#saveModel(String)
     */
    @Override
    public void saveModel(String filePath) {
    }

    /**
     * get Context
     *
     * @return recommender context
     */
    protected RecommenderContext getContext() {
        return context;
    }

    /**
     * set Context
     *
     * @param context recommender context
     */
    public void setContext(RecommenderContext context) {
        this.context = context;
    }

    /**
     * get Data Model
     *
     * @return data model
     */
    public DataModel getDataModel() {
        return context.getDataModel();
    }

    /**
     * get Recommended List
     *
     * @return Recommended List
     */
    public List<RecommendedItem> getRecommendedList() {
        if (recommendedList != null && recommendedList.size() > 0) {
            List<RecommendedItem> userItemList = new ArrayList<>();
            Iterator<UserItemRatingEntry> recommendedEntryIter = recommendedList.entryIterator();
            if (userMappingData != null && userMappingData.size() > 0 && itemMappingData != null && itemMappingData.size() > 0) {
                BiMap<Integer, String> userMappingInverse = userMappingData.inverse();
                BiMap<Integer, String> itemMappingInverse = itemMappingData.inverse();
                while (recommendedEntryIter.hasNext()) {
                    UserItemRatingEntry userItemRatingEntry = recommendedEntryIter.next();
                    if (userItemRatingEntry != null) {
                        String userId = userMappingInverse.get(userItemRatingEntry.getUserIdx());
                        String itemId = itemMappingInverse.get(userItemRatingEntry.getItemIdx());
                        if (StringUtils.isNotBlank(userId) && StringUtils.isNotBlank(itemId)) {
                            userItemList.add(new GenericRecommendedItem(userId, itemId, userItemRatingEntry.getValue()));
                        }
                    }
                }
                return userItemList;
            }
        }
        return null;
    }

    /**
     * Post each iteration, we do things:
     * <ol>
     * <li>print debug information</li>
     * <li>check if converged</li>
     * <li>if not, adjust learning rate</li>
     * </ol>
     *
     * @param iter current iteration
     * @return boolean: true if it is converged; false otherwise
     * @throws LibrecException if error occurs
     */
    protected boolean isConverged(int iter) throws LibrecException {
        float delta_loss = (float) (lastLoss - loss);

        // print out debug info
        if (verbose) {
            String recName = getClass().getSimpleName();
            String info = recName + " iter " + iter + ": loss = " + loss + ", delta_loss = " + delta_loss;
            LOG.info(info);
        }

        if (Double.isNaN(loss) || Double.isInfinite(loss)) {
//            LOG.error("Loss = NaN or Infinity: current settings does not fit the recommender! Change the settings and try again!");
            throw new LibrecException("Loss = NaN or Infinity: current settings does not fit the recommender! Change the settings and try again!");
        }

        // check if converged
        boolean converged = Math.abs(delta_loss) < 1e-5;

        return converged;
    }

    protected double computeEUI(int user, int item) {

        double eui = 0.0d;

        double[] tempImpWeight = implicitWeight[user];

        int[] implicitActions = getActions(user, item);
        if (implicitActions != null) {
            for (int idx = 0; idx < numActions; idx++) {
                eui += implicitActions[idx] * tempImpWeight[idx];
                actionCount[idx] += implicitActions[idx];
            }
        } else {
            eui += 0.0;
        }

        return eui;
    }

    protected double computeEU(int user) {

        double eu = 0.0d;

        actionCount = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};

        //item list to iterate
        List<Integer> itemList = trainMatrix.getColumnsImpFeedback(user);

        for (int itemIdx = 0; itemIdx < itemList.size(); itemIdx++) {
            eu += computeEUI(user, itemList.get(itemIdx));
        }

        return Maths.logistic(eu);
    }

    public int[] getActions(int userIdx, int itemIdx) {
        int[] actions = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};

        if (a1Action.contains(userIdx, itemIdx)) {
            actions[0] = 1;
        }
        if (a2Action.contains(userIdx, itemIdx)) {
            actions[1] = 1;
        }
        if (a3Action.contains(userIdx, itemIdx)) {
            actions[2] = 1;
        }
        if (a4Action.contains(userIdx, itemIdx)) {
            actions[3] = 1;
        }
        if (a5Action.contains(userIdx, itemIdx)) {
            actions[4] = 1;
        }
        if (a6Action.contains(userIdx, itemIdx)) {
            actions[5] = 1;
        }
        if (a7Action.contains(userIdx, itemIdx)) {
            actions[6] = 1;
        }
        if (a8Action.contains(userIdx, itemIdx)) {
            actions[7] = 1;
        }
        if (a9Action.contains(userIdx, itemIdx)) {
            actions[8] = 1;
        }
        return actions;
    }


}