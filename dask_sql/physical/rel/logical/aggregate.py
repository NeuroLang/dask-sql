import operator
from collections import defaultdict
from functools import reduce
from typing import Any, Callable, Dict, List, Tuple, Type
import logging

import pandas as pd
import dask.dataframe as dd

from dask_sql.utils import new_temporary_column
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer, ColumnContainer

logger = logging.getLogger(__name__)


class ReduceAggregation(dd.Aggregation):
    """
    A special form of an aggregation, that applies a given operation
    on all elements in a group with "reduce".
    """

    def __init__(self, name: str, operation: Callable):
        series_aggregate = lambda s: s.aggregate(
            lambda x: reduce(operation, x)
        )

        super().__init__(name, series_aggregate, series_aggregate)


class AggregationOnPandas(dd.Aggregation):
    """
    A special form of an aggregation, which does not apply the given function
    (given as attribute name) directly to the dask groupby, but
    via the groupby().apply() method. This is needed to call
    functions directly on the pandas dataframes, but should be done
    very carefully (as it is a performance bottleneck).
    """

    def __init__(self, function_name: str):
        def _f(s):
            return s.apply(lambda s0: getattr(s0.dropna(), function_name)())

        super().__init__(function_name, _f, _f)


class AggregationSpecification:
    """
    Most of the aggregations in SQL are already
    implemented 1:1 in dask and can just be called via their name
    (e.g. AVG is the mean). However sometimes those already
    implemented functions only work well for numerical
    functions. This small container class therefore
    can have an additional aggregation function, which is
    valid for non-numerical types.
    """

    def __init__(self, numerical_aggregation, non_numerical_aggregation=None):
        self.numerical_aggregation = numerical_aggregation
        self.non_numerical_aggregation = (
            non_numerical_aggregation or numerical_aggregation
        )


class LogicalAggregatePlugin(BaseRelPlugin):
    """
    A LogicalAggregate is used in GROUP BY clauses, but also
    when aggregating a function over the full dataset.

    In the first case we need to find out which columns we need to
    group over, in the second case we "cheat" and add a 1-column
    to the dataframe, which allows us to reuse every aggregation
    function we already know of.
    As NULLs are not groupable in dask, we handle them special
    by adding a temporary column which is True for all NULL values
    and False otherwise (and also group by it).

    The rest is just a lot of column-name-bookkeeping.
    Fortunately calcite will already make sure, that each
    aggregation function will only every be called with a single input
    column (by splitting the inner calculation to a step before).

    Open TODO: So far we are following the dask default
    to only have a single partition after the group by (which is usual
    a reasonable assumption). It would be nice to control
    these things via HINTs.
    """

    class_name = "org.apache.calcite.rel.logical.LogicalAggregate"

    AGGREGATION_MAPPING = {
        "$sum0": AggregationSpecification("sum", AggregationOnPandas("sum")),
        "sum": AggregationSpecification("sum", AggregationOnPandas("sum")),
        "any_value": AggregationSpecification(
            dd.Aggregation(
                "any_value",
                lambda s: s.sample(n=1).values,
                lambda s0: s0.sample(n=1).values,
            )
        ),
        "avg": AggregationSpecification("mean", AggregationOnPandas("mean")),
        "bit_and": AggregationSpecification(
            ReduceAggregation("bit_and", operator.and_)
        ),
        "bit_or": AggregationSpecification(
            ReduceAggregation("bit_or", operator.or_)
        ),
        "bit_xor": AggregationSpecification(
            ReduceAggregation("bit_xor", operator.xor)
        ),
        "count": AggregationSpecification("count"),
        "every": AggregationSpecification(
            dd.Aggregation("every", lambda s: s.all(), lambda s0: s0.all())
        ),
        "max": AggregationSpecification("max", AggregationOnPandas("max")),
        "min": AggregationSpecification("min", AggregationOnPandas("min")),
        "single_value": AggregationSpecification("first"),
    }

    def convert(
        self,
        rel: "org.apache.calcite.rel.RelNode",
        context: "dask_sql.Context",
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)

        df = dc.df
        cc = dc.column_container

        # We make our life easier with having unique column names
        cc = cc.make_unique()

        # I have no idea what that is, but so far it was always of length 1
        assert (
            len(rel.getGroupSets()) == 1
        ), "Do not know how to handle this case!"

        # Extract the information, which columns we need to group for
        group_column_indices = [int(i) for i in rel.getGroupSet()]
        group_columns = [
            cc.get_backend_by_frontend_index(i) for i in group_column_indices
        ]

        dc = DataContainer(df, cc)

        if not group_columns:
            # There was actually no GROUP BY specified in the SQL
            # Still, this plan can also be used if we need to aggregate something over the full
            # data sample
            # To reuse the code, we just create a new column at the end with a single value
            logger.debug("Performing full-table aggregation")

        # Do all aggregates
        df_result, output_column_order = self._do_aggregations(
            rel,
            dc,
            group_columns,
            context,
        )

        # SQL does not care about the index, but we do not want to have any multiindices
        df_agg = df_result.reset_index(drop=True)

        # Fix the column names and the order of them, as this was messed with during the aggregations
        df_agg.columns = df_agg.columns.get_level_values(-1)
        cc = ColumnContainer(df_agg.columns).limit_to(output_column_order)

        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(df_agg, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc

    def _do_aggregations(
        self,
        rel: "org.apache.calcite.rel.RelNode",
        dc: DataContainer,
        group_columns: List[str],
        context: "dask_sql.Context",
    ) -> Tuple[dd.DataFrame, List[str]]:
        """
        Main functionality: return the result dataframe
        and the output column order
        """
        df = dc.df
        cc = dc.column_container

        # We might need it later.
        # If not, lets hope that adding a single column should not
        # be a huge problem...
        additional_column_name = new_temporary_column(df)
        df = df.assign(**{additional_column_name: 1})

        # Add an entry for every grouped column, as SQL wants them first
        output_column_order = group_columns.copy()

        # Collect all aggregations we need to do
        (
            collected_aggregations,
            output_column_order,
        ) = self._collect_aggregations(
            rel, df, cc, context, additional_column_name, output_column_order
        )

        # SQL needs to have a column with the grouped values as the first
        # output column. As the values of the group columns
        # are the same for a single group anyways, we just use the first row.
        df_result = None
        if group_columns:
            default_column_aggregations = []
            for col in group_columns:
                default_column_aggregations.append((col, col, "first", None))
            df_result = self._apply_and_assign_aggregations(
                df,
                df_result,
                None,
                default_column_aggregations,
                additional_column_name,
                group_columns,
            )

        # Now we can go ahead and use these grouped aggregations
        # to perform the actual aggregation
        # It is very important to start with the non-filtered entry.
        # Otherwise we might loose some entries in the grouped columns
        if None in collected_aggregations:
            key = None
            aggregations = collected_aggregations.pop(key)
            df_result = self._apply_and_assign_aggregations(
                df,
                df_result,
                None,
                aggregations,
                additional_column_name,
                group_columns,
            )

        # Now we can also add the rest
        for filter_column, aggregations in collected_aggregations.items():
            df_result = self._apply_and_assign_aggregations(
                df,
                df_result,
                filter_column,
                aggregations,
                additional_column_name,
                group_columns,
            )

        return df_result, output_column_order

    def _apply_and_assign_aggregations(
        self,
        df: dd.DataFrame,
        df_result: dd.DataFrame,
        filter_column: str,
        aggregations: List[Tuple[str, str, Any, Type]],
        additional_column_name: str,
        group_columns: List[str],
    ):
        agg_result = self._perform_aggregation(
            df,
            filter_column,
            aggregations,
            additional_column_name,
            group_columns,
        )
        if df_result is None:
            df_result = agg_result
        else:
            df_result = df_result.assign(
                **{col: agg_result[col] for col in agg_result.columns}
            )
        return df_result

    def _collect_aggregations(
        self,
        rel: "org.apache.calcite.rel.RelNode",
        df: dd.DataFrame,
        cc: ColumnContainer,
        context: "dask_sql.Context",
        additional_column_name: str,
        output_column_order: List[str],
    ) -> Tuple[
        Dict[Tuple[str, str], List[Tuple[str, str, Any, Type]]], List[str]
    ]:
        """
        Collect all aggregations together, which have the same filter column
        so that the aggregations only need to be done once.

        Returns the aggregations as mapping filter_column -> List of Aggregations
        where the aggregations are in the form (input_col, output_col, aggregation function (or string), return_type)
        """
        collected_aggregations = defaultdict(list)

        for agg_call in rel.getNamedAggCalls():
            expr = agg_call.getKey()

            # Find out about the input column
            inputs = expr.getArgList()
            if len(inputs) == 1:
                input_col = cc.get_backend_by_frontend_index(inputs[0])
            elif len(inputs) == 0:
                input_col = additional_column_name
            else:
                input_col = tuple(
                    cc.get_backend_by_frontend_index(i) for i in inputs
                )

            # Extract flags (filtering/distinct)
            if expr.isDistinct():  # pragma: no cover
                raise ValueError("Apache Calcite should optimize them away!")

            filter_column = None
            if expr.hasFilter():
                filter_column = cc.get_backend_by_frontend_index(
                    expr.filterArg
                )

            # Find out which aggregation function to use
            aggregation_name = str(expr.getAggregation().getName())
            aggregation_name = aggregation_name.lower()
            return_type = None
            for function_description in context.function_list:
                if function_description.name == aggregation_name:
                    return_type = function_description.return_type
            try:
                aggregation_function = self.AGGREGATION_MAPPING[
                    aggregation_name
                ]
            except KeyError:
                try:
                    aggregation_function = context.functions[aggregation_name]
                except KeyError:  # pragma: no cover
                    raise NotImplementedError(
                        f"Aggregation function {aggregation_name} not implemented (yet)."
                    )
            if isinstance(aggregation_function, AggregationSpecification):
                if isinstance(input_col, tuple):
                    dtype = df[input_col[0]].dtype
                else:
                    dtype = df[input_col].dtype
                if pd.api.types.is_numeric_dtype(dtype):
                    aggregation_function = (
                        aggregation_function.numerical_aggregation
                    )
                else:
                    aggregation_function = (
                        aggregation_function.non_numerical_aggregation
                    )

            # Finally, extract the output column name
            output_col = str(agg_call.getValue())

            # Store the aggregation
            key = filter_column
            value = (input_col, output_col, aggregation_function, return_type)
            collected_aggregations[key].append(value)
            output_column_order.append(output_col)

        return collected_aggregations, output_column_order

    def _perform_aggregation(
        self,
        df: dd.DataFrame,
        filter_column: str,
        aggregations: List[Tuple[str, str, Any, Type]],
        additional_column_name: str,
        group_columns: List[str],
    ):
        tmp_df = df

        if filter_column:
            filter_expression = tmp_df[filter_column]
            tmp_df = tmp_df[filter_expression]

            logger.debug(f"Filtered by {filter_column} before aggregation.")

        # Jonas : we don't really care to have the exact same behaviour as SQL
        # and grouping by series instead of column names is messing up the
        # multi column aggregations so i'm just assuming this will work
        # instead of the commented part below.
        group_columns_and_nulls = group_columns

        # # SQL and dask are treating null columns a bit different:
        # # SQL will put them to the front, dask will just ignore them
        # # Therefore we use the same trick as fugue does:
        # # we will group by both the NaN and the real column value
        # group_columns_and_nulls = []
        # for group_column in group_columns:
        #     # the ~ makes NaN come first
        #     is_null_column = ~(tmp_df[group_column].isnull())
        #     non_nan_group_column = tmp_df[group_column].fillna(0)

        #     group_columns_and_nulls += [is_null_column, non_nan_group_column]

        if not group_columns_and_nulls:
            # This can happen in statements like
            # SELECT SUM(x) FROM data
            # without any groupby statement
            group_columns_and_nulls = [additional_column_name]

        grouped_df = tmp_df.groupby(by=group_columns_and_nulls)

        # Dask supports two types of group-by aggregations: by calling .agg
        # or .apply on a GroupBy dataframe. We want to call .agg if possible,
        # as it's supposed to be faster en cleaner. But it doesn't work for
        # all cases, in which case we use .apply instead. We start by
        # preparing the aggregate calls in a format dask understands.
        aggregate_aggregations = defaultdict(dict)
        apply_aggregations = dict()
        for aggregation in aggregations:
            input_col, output_col, aggregation_f, return_type = aggregation
            if isinstance(
                aggregation_f, (AggregationSpecification, dd.Aggregation, str)
            ):
                aggregate_aggregations[input_col][output_col] = aggregation_f
            else:
                apply_aggregations[output_col] = (
                    input_col,
                    aggregation_f,
                    return_type,
                )

        # Now we apply the aggregations
        agg_result = None
        if len(aggregate_aggregations) > 0:
            logger.debug(f"Performing aggregation {dict(aggregate_aggregations)}")
            agg_result = grouped_df.agg(aggregate_aggregations)

            # ... fix the column names to a single level ...
            agg_result.columns = agg_result.columns.get_level_values(-1)

        # apply aggregations with .apply
        for output_col, (
            input_col,
            aggregation_f,
            return_type,
        ) in apply_aggregations.items():
            new_col = grouped_df.apply(
                lambda x: aggregation_f(
                    *[getattr(x, col) for col in input_col]
                ),
                meta=(output_col, return_type)
            )
            if agg_result is None:
                agg_result = new_col.rename(output_col).to_frame()
            else:
                agg_result = agg_result.assign(**{output_col: new_col})

        return agg_result
