package net.librec.recommender;

import net.librec.common.LibrecException;
import net.librec.math.algorithm.Maths;
import net.librec.math.algorithm.Randoms;
import net.librec.math.structure.DenseMatrix;

import java.util.*;

/**
 * Matrix Factorization Recommender
 * Methods with user factors and item factors: such as SVD(Singular Value Decomposition)
 * <p>
 * Created by Keqiang Wang
 */
public abstract class MatrixFactorizationsRecommender extends AbstractRecommenders {
    /**
     * learn rate, maximum learning rate
     */
    protected float learnRate, maxLearnRate;

    /**
     * user latent factors
     */
    protected DenseMatrix userFactors;

    /**
     * item latent factors
     */
    protected DenseMatrix itemFactors;

    /**
     * the number of latent factors;
     */
    protected int numFactors;

    /**
     * the number of iterations
     */
    protected int numIterations;

    /**
     * init mean
     */
    protected float initMean;

    /**
     * init standard deviation
     */
    protected float initStd;

    /**
     * user regularization
     */
    protected float regUser;

    /**
     * item regularization
     */
    protected float regItem;

    /**
     * implicit weight regularization
     */
    protected float regWeight;

    protected float a;

    /**
     * setup
     * init member method
     *
     * @throws LibrecException if error occurs during setting up
     */
    protected void setup() throws LibrecException {
        super.setup();

        itemPops = new HashMap<>();

        numIterations = conf.getInt("rec.iterator.maximum", 100);
        learnRate = conf.getFloat("rec.iterator.learnrate", 0.01f);
        maxLearnRate = conf.getFloat("rec.iterator.learnrate.maximum", 0.01f);

        regUser = conf.getFloat("rec.user.regularization", 0.01f);
        regItem = conf.getFloat("rec.item.regularization", 0.01f);
        regWeight = conf.getFloat("rec.implicit.regularization", 0.001f);

        numFactors = conf.getInt("rec.factor.number", 10);
        isBoldDriver = conf.getBoolean("rec.learnrate.bolddriver", false);
        decay = conf.getFloat("rec.learnrate.decay", 1.0f);
        a = conf.getFloat("a", 1.0f);

        userFactors = new DenseMatrix(numUsers, numFactors);
        itemFactors = new DenseMatrix(numItems, numFactors);

        //initialize weight
        List<Double> impWeight = Randoms.list(numActions, 0, 1, false);
        double[] tempImpWeight = new double[numActions];

        for (int idx = 0; idx < numActions; idx++) {
            tempImpWeight[idx] = impWeight.get(idx);
        }

        try {
            tempImpWeight = Maths.softmax(tempImpWeight);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int uIdx = 0; uIdx < numUsers; uIdx++) {
            for (int fIdx = 0; fIdx < numActions; fIdx++) {
                implicitWeight[uIdx][fIdx] = tempImpWeight[fIdx];
            }
        }

        // initialize factors
        initMean = 0.0f;
        initStd = 0.5f;
//        initStd = conf.getFloat("std", 0.1f);

        userFactors.init(initMean, initStd);
        itemFactors.init(initMean, initStd);

    }


    /**
     * predict a specific rating for user userIdx on item itemIdx.
     *
     * @param userIdx user index
     * @param itemIdx item index
     * @return predictive rating for user userIdx on item itemIdx with bound
     * @throws LibrecException if error occurs during predicting
     */
    protected double mfPredict(int userIdx, int itemIdx){
        double mf = DenseMatrix.rowMult(userFactors, userIdx, itemFactors, itemIdx);

        mf = Pk * mf + Pb;
        return Maths.logistic(mf);
    }

    protected double predict(int userIdx, int itemIdx) throws LibrecException {

        if (!itemPops.containsKey(itemIdx)) {
            double itemPopularity = computePopualrity(itemIdx, userIdx);
            itemPops.put(itemIdx, itemPopularity);
        }

        return 0;
    }

    @Override
    protected double predict(int userIdx, int itemIdx, double eui) throws LibrecException {
        if (!itemPops.containsKey(itemIdx)) {
            double itemPopularity = computePopualrity(userIdx, itemIdx);
            itemPops.put(itemIdx, itemPopularity);
        }
        return a * itemPops.get(itemIdx) + mfPredict(userIdx, itemIdx) + (1 - a) * eui;
    }



    /**
     * Update current learning rate after each epoch <br>
     * <ol>
     * <li>bold driver: Gemulla et al., Large-scale matrix factorization with distributed stochastic gradient descent,
     * KDD 2011.</li>
     * <li>constant decay: Niu et al, Hogwild!: A lock-free approach to parallelizing stochastic gradient descent, NIPS
     * 2011.</li>
     * <li>Leon Bottou, Stochastic Gradient Descent Tricks</li>
     * <li>more ways to adapt learning rate can refer to: http://www.willamette.edu/~gorr/classes/cs449/momrate.html</li>
     * </ol>
     * @param iter the current iteration
     */
    protected void updateLRate(int iter) {
        if (learnRate < 0.0) {
            return;
        }

        if (isBoldDriver && iter > 1) {
            learnRate = Math.abs(lastLoss) > Math.abs(loss) ? learnRate * 1.05f : learnRate * 0.5f;
        } else if (decay > 0 && decay < 1) {
            learnRate *= decay;
        }

        // limit to max-learn-rate after update
        if (maxLearnRate > 0 && learnRate > maxLearnRate) {
            learnRate = maxLearnRate;
        }
        lastLoss = loss;

    }
}
