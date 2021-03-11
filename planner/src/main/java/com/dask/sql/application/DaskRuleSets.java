package com.dask.sql.application;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * RuleSets and utilities for creating Programs to use with Calcite's query
 * planners. This is inspired both from Apache Calcite's default optimization
 * programs
 * (https://github.com/apache/calcite/blob/master/core/src/main/java/org/apache/calcite/tools/Programs.java)
 * and Apache Flink's multi-phase query optimization
 * (https://github.com/apache/flink/blob/master/flink-table/flink-table-planner-blink/src/main/scala/org/apache/flink/table/planner/plan/optimize/program/FlinkStreamProgram.scala)
 */
public class DaskRuleSets {

        // private constructor
        private DaskRuleSets() {
        }

        /**
         * RuleSet to reduce expressions
         */
        static final RuleSet REDUCE_EXPRESSION_RULES = RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS,
                        CoreRules.PROJECT_REDUCE_EXPRESSIONS, CoreRules.CALC_REDUCE_EXPRESSIONS,
                        CoreRules.JOIN_REDUCE_EXPRESSIONS, CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

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
         * RuleSet about project Dont' add CoreRules.PROJECT_REMOVE
         */
        static final RuleSet PROJECT_RULES = RuleSets.ofList(CoreRules.PROJECT_MERGE, CoreRules.AGGREGATE_PROJECT_MERGE,
                        // push a projection past a filter
                        CoreRules.PROJECT_FILTER_TRANSPOSE,
                        // merge projections
                        CoreRules.PROJECT_MERGE,
                        // removes constant keys from an Agg
                        CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
                        // push project through a Union
                        CoreRules.PROJECT_SET_OP_TRANSPOSE, CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,
                        CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE, CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE);

        /**
         * RuleSet about aggregate
         */
        static final RuleSet AGGREGATE_RULES = RuleSets.ofList(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
                        CoreRules.AGGREGATE_JOIN_TRANSPOSE, CoreRules.AGGREGATE_PROJECT_MERGE,
                        CoreRules.PROJECT_AGGREGATE_MERGE, CoreRules.AGGREGATE_MERGE,
                        // Important. Removes unecessary distinct calls
                        CoreRules.AGGREGATE_REMOVE, CoreRules.AGGREGATE_JOIN_REMOVE);

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
        static final RuleSet JOIN_REORDER_RULES = RuleSets.ofList(
                        // optimize multi joins
                        CoreRules.MULTI_JOIN_OPTIMIZE,
                        // optmize bushy multi joins
                        CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);

        /**
         * Rules to reorder joins using associate and commute rules. See
         * https://www.querifylabs.com/blog/rule-based-query-optimization for an
         * explanation. JoinCommuteRule causes exhaustive search and should probably not
         * be used.
         */
        static final RuleSet JOIN_COMMUTE_ASSOCIATE_RULES = RuleSets.ofList(
                        // changes a join based on associativity rule.
                        CoreRules.JOIN_ASSOCIATE, CoreRules.JOIN_COMMUTE);

        /**
         * RuleSet to do logical optimize.
         */
        static final RuleSet LOGICAL_RULES = RuleSets.ofList(
                        // remove union with only a single child
                        CoreRules.UNION_REMOVE,
                        // convert non-all union into all-union + distinct
                        CoreRules.UNION_TO_DISTINCT, CoreRules.MINUS_MERGE,
                        // aggregation and projection rules
                        // CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
                        // CoreRules.AGGREGATE_REMOVE, CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,
                        CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST, CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND);

        /**
         * Initial rule set from dask_sql with a couple rules added by Demian. Not used
         * but kept for reference.
         */
        static final RuleSet DASK_DEFAULT_CORE_RULES = RuleSets.ofList(
                        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN, CoreRules.FILTER_SET_OP_TRANSPOSE,
                        CoreRules.FILTER_AGGREGATE_TRANSPOSE, CoreRules.FILTER_INTO_JOIN, CoreRules.JOIN_CONDITION_PUSH,
                        CoreRules.PROJECT_JOIN_TRANSPOSE, CoreRules.PROJECT_MULTI_JOIN_MERGE,
                        CoreRules.JOIN_TO_MULTI_JOIN, CoreRules.MULTI_JOIN_OPTIMIZE,
                        CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY, CoreRules.AGGREGATE_JOIN_TRANSPOSE,
                        CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.PROJECT_AGGREGATE_MERGE, CoreRules.AGGREGATE_MERGE,
                        CoreRules.PROJECT_MERGE, CoreRules.FILTER_MERGE,
                        // Don't add this rule as it removes projections which are used to rename colums
                        // CoreRules.PROJECT_REMOVE,
                        CoreRules.PROJECT_REDUCE_EXPRESSIONS, CoreRules.FILTER_REDUCE_EXPRESSIONS,
                        CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM, CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

        /**
         * Builds a HepProgram for the given set of rules and with the given order. If
         * type is COLLECTION, rules are added as collection. Otherwise, rules are added
         * sequentially.
         */
        public static HepProgram hepProgram(final RuleSet rules, final HepMatchOrder order,
                        final HepExecutionType type) {
                final HepProgramBuilder builder = new HepProgramBuilder().addMatchOrder(order);
                switch (type) {
                case SEQUENCE:
                        for (RelOptRule rule : rules) {
                                builder.addRuleInstance(rule);
                        }
                        break;
                case COLLECTION:
                        List<RelOptRule> rulesCollection = new ArrayList<RelOptRule>();
                        rules.iterator().forEachRemaining(rulesCollection::add);
                        builder.addRuleCollection(rulesCollection);
                        break;
                }
                return builder.build();
        }

        public enum HepExecutionType {
                SEQUENCE, COLLECTION
        }

}