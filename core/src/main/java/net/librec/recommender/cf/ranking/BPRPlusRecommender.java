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
package net.librec.recommender.cf.ranking;

import net.librec.annotation.ModelData;
import net.librec.common.LibrecException;
import net.librec.math.algorithm.Maths;
import net.librec.math.algorithm.Randoms;
import net.librec.math.structure.SparseMatrix;
import net.librec.recommender.MatrixFactorizationsRecommender;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rendle et al., <strong>BPR: Bayesian Personalized Ranking from Implicit Feedback</strong>, UAI 2009.
 *
 *
 */
@ModelData({"isRanking", "bprplus", "userFactors", "itemFactors"})
public class BPRPlusRecommender extends MatrixFactorizationsRecommender {
    private List<Set<Integer>> userItemsSet;

    @Override
    protected void setup() throws LibrecException {
        super.setup();
    }

    @Override
    protected void trainModel() throws Exception {

        userItemsSet = getUserItemsSet(trainMatrix);
//        userImpItemsSet = getUserImpItemsSet(trainMatrix);

        for (int iter = 1; iter <= numIterations; iter++) {

            loss = 0.0d;
            loss_train=0.0d;
            loss_reg=0.0d;
            for (int sampleCount = 0, smax = numUsers * 10; sampleCount < smax; sampleCount++) {

                // randomly draw (userIdx)
                int userIdx, posItemIdx, negItemIdx;

                while (true) {
                    userIdx = Randoms.uniform(numUsers);

                    //purchased item set for the user
                    Set<Integer> itemSet = userItemsSet.get(userIdx);
                    if (itemSet.size() == 0 || itemSet.size() == numItems)
                        continue;

                    //random draw posItemIdx
                    List<Integer> itemList = trainMatrix.getColumns(userIdx);
                    posItemIdx = itemList.get(Randoms.uniform(itemList.size()));

                    do {
                        negItemIdx = Randoms.uniform(numItems);
                    } while (itemSet.contains(negItemIdx));

                    break;
                }

                // update parameters

                double posEu = computeEUI(userIdx, posItemIdx);
                double negEu = computeEUI(userIdx, negItemIdx);

                double posPredictRating = predict(userIdx, posItemIdx, posEu);
                double negPredictRating = predict(userIdx, negItemIdx, negEu);
                double diffValue = posPredictRating - negPredictRating;

                double posMf = mfPredict(userIdx, posItemIdx);
                double negMf = mfPredict(userIdx, negItemIdx);

                double lossValue = -Math.log(Maths.logistic(diffValue));
                loss += lossValue;
                loss_train+=lossValue;
                double deriValue = Maths.logistic(-diffValue);

                for (int factorIdx = 0; factorIdx < numFactors; factorIdx++) {
                    double userFactorValue = userFactors.get(userIdx, factorIdx);
                    double posItemFactorValue = itemFactors.get(posItemIdx, factorIdx);
                    double negItemFactorValue = itemFactors.get(negItemIdx, factorIdx);

                    double userFactorUpdate = Pk * (posMf * (1 - posMf) * posItemFactorValue - negMf * (1 - negMf) * negItemFactorValue);
                    userFactors.add(userIdx, factorIdx, learnRate * (deriValue * userFactorUpdate - regUser * userFactorValue));
                    double positemFactorUpdate = Pk * posMf * (1 - posMf) * userFactorValue;
                    itemFactors.add(posItemIdx, factorIdx, learnRate * (deriValue * positemFactorUpdate - regItem * posItemFactorValue));
                    double negitemFactorUpdate = -Pk * negMf * (1- negMf) * userFactorValue;
                    itemFactors.add(negItemIdx, factorIdx, learnRate * (deriValue * negitemFactorUpdate - regItem * negItemFactorValue));

                    loss += regUser * userFactorValue * userFactorValue + regItem * posItemFactorValue * posItemFactorValue + regItem * negItemFactorValue * negItemFactorValue;
                    loss_reg += regUser * userFactorValue * userFactorValue + regItem * posItemFactorValue * posItemFactorValue + regItem * negItemFactorValue * negItemFactorValue;
                }

                double[] tempImpWeight = new double[numActions];

                int[] posAction = getActions(userIdx, posItemIdx);
                int[] negAction = getActions(userIdx, negItemIdx);

                double posPopularity;
                double negPopularity;

                int[] posItemCount = getItemCount(posItemIdx);
                int[] negItemCount = getItemCount(negItemIdx);

                for (int impWeightIdx = 0; impWeightIdx < numActions; impWeightIdx++) {
                    double impWeightValue = implicitWeight[userIdx][impWeightIdx];

                    if (itemPops.containsKey(posItemIdx)){
                        posPopularity = itemPops.get(posItemIdx);
                    }else {
                        posPopularity = computePopualrity(userIdx, posItemIdx);
                        itemPops.put(posItemIdx, posPopularity);
                    }

                    if (itemPops.containsKey(negItemIdx)) {
                        negPopularity = itemPops.get(negItemIdx);
                    }else{
                        negPopularity = computePopualrity(userIdx, negItemIdx);
                        itemPops.put(negItemIdx, negPopularity);
                    }

                    double aPartUpdate = a * (posPopularity * (1 - posPopularity) * posItemCount[impWeightIdx] * Pok - posEu * (1 - posEu) * posAction[impWeightIdx] * Ek - negPopularity * (1 - negPopularity) * negItemCount[impWeightIdx] * Pok + negEu * (1 - negEu) * negAction[impWeightIdx] * Ek);
                    double otherPartUpdate = posEu * (1 - posEu) * posAction[impWeightIdx] * Ek - negEu * (1 - negEu) * negAction[impWeightIdx] * Ek;
//                    double popUpdate = a * Pok * (posPopularity * (1 - posPopularity) * posItemCount[impWeightIdx] - negPopularity * (1 - negPopularity) * negItemCount[impWeightIdx]);
//                    double experUpdate = Ek * (posEu * (1 - posEu) * posAction[impWeightIdx] - negEu * (1 - negEu) * negAction[impWeightIdx]);
                    double impWeightUpdate =  aPartUpdate + otherPartUpdate;
                    impWeightValue += learnRate * (deriValue * impWeightUpdate - regWeight * impWeightValue);

                    tempImpWeight[impWeightIdx] = impWeightValue;

                    loss += regWeight * impWeightValue * impWeightValue;
                    loss_reg += regWeight * impWeightValue * impWeightValue;
                }

                tempImpWeight = Maths.softmax(tempImpWeight);

                for (int impWeightIdx = 0; impWeightIdx < numActions; impWeightIdx++) {
                    implicitWeight[userIdx][impWeightIdx] = tempImpWeight[impWeightIdx];
//                    loss += regWeight * tempImpWeight[impWeightIdx] * tempImpWeight[impWeightIdx];
                }
            }

            if (isConverged(iter) && earlyStop) {
                break;
            }
            updateLRate(iter);
//            printTrainInfo();

//            if (iter == 47){

//            }
        }
//        for (int u = 0; u < numUsers; u++) {
//            System.out.print("\n");
//            for (int impWeightIdx = 0; impWeightIdx < numActions; impWeightIdx++) {
//                System.out.print(implicitWeight[u][impWeightIdx]+",");
//            }
//        }
//        System.out.println("A");

    }

    private List<Set<Integer>> getUserItemsSet(SparseMatrix sparseMatrix) {
        List<Set<Integer>> userItemsSet = new ArrayList<>();
        for (int userIdx = 0; userIdx < numUsers; ++userIdx) {
            userItemsSet.add(new HashSet(sparseMatrix.getColumns(userIdx)));
        }
        return userItemsSet;
    }

}
