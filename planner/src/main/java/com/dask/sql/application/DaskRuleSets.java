package com.dask.sql.application;

import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class DaskRuleSets {
    /**
     * Convert sub-queries before query decorrelation.
     */
    static final RuleSet TABLE_SUBQUERY_RULES = RuleSets.ofList(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
            CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE, CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);

    /**
     * RuleSet to reduce expressions
     */
    static final RuleSet REDUCE_EXPRESSION_RULES = RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS, CoreRules.CALC_REDUCE_EXPRESSIONS, CoreRules.JOIN_REDUCE_EXPRESSIONS,
            CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

    /**
     * RuleSet about filter
     */
    static final RuleSet FILTER_RULES = RuleSets.ofList(
            // push a filter into a join
            CoreRules.FILTER_INTO_JOIN,
            // push filter into the children of a join
            CoreRules.JOIN_CONDITION_PUSH,
            // push filter through an aggregation
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
            // push a filter past a project
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            // push a filter past a setop
            CoreRules.FILTER_SET_OP_TRANSPOSE, CoreRules.FILTER_MERGE);

    /**
     * RuleSet about project
     */
    static final RuleSet PROJECT_RULES = RuleSets.ofList(
            // push a projection past a filter
            CoreRules.PROJECT_FILTER_TRANSPOSE,
            // merge projections
            CoreRules.PROJECT_MERGE,
            // Don't add PROJECT_REMOVE
            // CoreRules.PROJECT_REMOVE,
            // removes constant keys from an Agg
            CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
            // push project through a Union
            CoreRules.PROJECT_SET_OP_TRANSPOSE, CoreRules.PROJECT_JOIN_TRANSPOSE);

    /**
     * RuleSet about aggregate
     */
    static final RuleSet AGGREGATE_RULES = RuleSets.ofList(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
            CoreRules.AGGREGATE_JOIN_TRANSPOSE, CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.PROJECT_AGGREGATE_MERGE,
            CoreRules.AGGREGATE_MERGE);

    /**
     * RuleSet for merging joins
     */
    static final RuleSet JOIN_REORDER_PREPARE_RULES = RuleSets.ofList(
            // merge project to MultiJoin
            CoreRules.PROJECT_MULTI_JOIN_MERGE,
            // merge filter to MultiJoin
            CoreRules.FILTER_MULTI_JOIN_MERGE,
            // merge join to MultiJoin
            CoreRules.JOIN_TO_MULTI_JOIN);

    /**
     * Rules to reorder joins
     */
    static final RuleSet JOIN_REORDER_RULES = RuleSets.ofList(CoreRules.MULTI_JOIN_OPTIMIZE,
            CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);

    /**
     * RuleSet to do logical optimize.
     */
    static final RuleSet LOGICAL_RULES = RuleSets.ofList(
            // scan optimization
            // PushProjectIntoTableSourceScanRule.INSTANCE,
            // PushProjectIntoLegacyTableSourceScanRule.INSTANCE,
            // PushFilterIntoTableSourceScanRule.INSTANCE,
            // PushFilterIntoLegacyTableSourceScanRule.INSTANCE,
            // PushLimitIntoTableSourceScanRule.INSTANCE,

            // reorder sort and projection
            // CoreRules.SORT_PROJECT_TRANSPOSE,
            // remove unnecessary sort rule
            // CoreRules.SORT_REMOVE,

            // join rules
            // FlinkJoinPushExpressionsRule.INSTANCE,
            // SimplifyJoinConditionRule.INSTANCE,

            // remove union with only a single child
            CoreRules.UNION_REMOVE,
            // convert non-all union into all-union + distinct
            CoreRules.UNION_TO_DISTINCT,

            // aggregation and projection rules
            CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,

            // remove aggregation if it does not aggregate and input is already distinct
            // FlinkAggregateRemoveRule.INSTANCE,
            // push aggregate through join
            // FlinkAggregateJoinTransposeRule.EXTENDED,
            // using variants of aggregate union rule
            CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST, CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND

    // reduce aggregate functions like AVG, STDDEV_POP etc.
    // CoreRules.AGGREGATE_REDUCE_FUNCTIONS,

    // reduce useless aggCall
    // PruneAggregateCallRule.PROJECT_ON_AGGREGATE,
    // PruneAggregateCallRule.CALC_ON_AGGREGATE,

    // expand grouping sets
    // DecomposeGroupingSetsRule.INSTANCE,

    // calc rules
    // CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE,
    // CoreRules.FILTER_TO_CALC,
    // CoreRules.PROJECT_TO_CALC
    // FlinkCalcMergeRule.INSTANCE,

    // semi/anti join transpose rule
    // FlinkSemiAntiJoinJoinTransposeRule.INSTANCE,
    // FlinkSemiAntiJoinProjectTransposeRule.INSTANCE,
    // FlinkSemiAntiJoinFilterTransposeRule.INSTANCE,

    // set operators
    // ReplaceIntersectWithSemiJoinRule.INSTANCE,
    // RewriteIntersectAllRule.INSTANCE,
    // ReplaceMinusWithAntiJoinRule.INSTANCE,
    // RewriteMinusAllRule.INSTANCE
    );

    /**
     * Initial rule set from dask_sql with a couple rules added by Demian.
     */
    static final RuleSet DASK_DEFAULT_CORE_RULES = RuleSets.ofList(
            CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN, CoreRules.FILTER_SET_OP_TRANSPOSE,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE, CoreRules.FILTER_INTO_JOIN, CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.PROJECT_JOIN_TRANSPOSE, CoreRules.PROJECT_MULTI_JOIN_MERGE, CoreRules.JOIN_TO_MULTI_JOIN,
            CoreRules.MULTI_JOIN_OPTIMIZE, CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY, CoreRules.AGGREGATE_JOIN_TRANSPOSE,
            CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.PROJECT_AGGREGATE_MERGE, CoreRules.AGGREGATE_MERGE,
            CoreRules.PROJECT_MERGE, CoreRules.FILTER_MERGE,
            // Don't add this rule as it removes projections which are used to rename colums
            // CoreRules.PROJECT_REMOVE,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS, CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM, CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

}