// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Optional;

public class PushDownAggFunPredicateRule extends TransformationRule {

    public PushDownAggFunPredicateRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_FUN_PREDICATE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregateOperator = (LogicalAggregationOperator) input.getOp();
        if (aggregateOperator.getAggregations().size() != 1) {
            return Lists.newArrayList();
        }
        Optional<ScalarOperator> pushDownPrdOp = buildScalarPrdFormAggFunPrd(aggregateOperator);
        if (pushDownPrdOp.isEmpty()) {
            return Lists.newArrayList();
        }
        ScalarOperator pushDownPrd = pushDownPrdOp.get();
        OptExpression pushDownFilter = new OptExpression(new LogicalFilterOperator(pushDownPrd));
        pushDownFilter.getInputs().addAll(input.getInputs());
        input.getInputs().clear();
        input.getInputs().add(pushDownFilter);
        return Lists.newArrayList(input);
    }

    Optional<ScalarOperator> buildScalarPrdFormAggFunPrd(LogicalAggregationOperator logicalAggOperator) {
        if (logicalAggOperator.getAggregations().size() != 1) {
            return Optional.empty();
        }
        List<ScalarOperator> sourcePrd = Utils.extractConjuncts(logicalAggOperator.getPredicate());
        for (ScalarOperator so : sourcePrd) {
            if (so instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binarySo = (BinaryPredicateOperator) so;
                ScalarOperator key = binarySo.getChild(0);
                CallOperator aggFun = logicalAggOperator.getAggregations().get(key);
                if (aggFun != null) {
                    BinaryPredicateOperator pushDownPredicate = null;
                    if (aggFun.getFnName().equalsIgnoreCase("min")) {
                        if (binarySo.getBinaryType().equals(BinaryType.EQ)) {
                            pushDownPredicate = new BinaryPredicateOperator(BinaryType.LE,
                                    aggFun.getChildren().get(0), binarySo.getChild(1));
                        } else if (binarySo.getBinaryType().equals(BinaryType.LT) ||
                                binarySo.getBinaryType().equals(BinaryType.LE)) {
                            pushDownPredicate = new BinaryPredicateOperator(binarySo.getBinaryType(),
                                    aggFun.getChildren().get(0), binarySo.getChild(1));
                        }
                    } else if (aggFun.getFnName().equalsIgnoreCase("max")) {
                        if (binarySo.getBinaryType().equals(BinaryType.EQ)) {
                            pushDownPredicate = new BinaryPredicateOperator(BinaryType.GE,
                                    aggFun.getChildren().get(0), binarySo.getChild(1));
                        } else if (binarySo.getBinaryType().equals(BinaryType.GT) ||
                                binarySo.getBinaryType().equals(BinaryType.GE)) {
                            pushDownPredicate = new BinaryPredicateOperator(binarySo.getBinaryType(),
                                    aggFun.getChildren().get(0), binarySo.getChild(1));
                        }
                    }
                    if (pushDownPredicate != null && !so.getHints().contains("PushDownAggFunPrdHint")) {
                        List newHints = Lists.newArrayList(so.getHints());
                        newHints.add("PushDownAggFunPrdHint");
                        so.setHints(newHints);
                        return Optional.of(pushDownPredicate);
                    }
                }
            }
        }
        return Optional.empty();
    }
}
