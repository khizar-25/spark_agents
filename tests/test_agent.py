import pytest
from agent.state_detector import StateDetector, SparkPipelineState
from agent.goal_comparator import GoalComparator
from agent.tool_validator import ToolValidator
from config.goal_state import etl_goal

def test_state_initial(): assert StateDetector().get_state().pipeline_stage == "idle"
def test_state_reset():
    sd = StateDetector(); sd.mark_pipeline_stage("done"); sd.reset()
    assert sd.get_state().pipeline_stage == "idle"
def test_goal_incomplete():
    r = GoalComparator().compare(etl_goal("s3://in","s3://out"), SparkPipelineState())
    assert not r.is_complete
def test_goal_complete():
    goal = etl_goal("s3://in","s3://out"); goal.dq_rules={}; goal.transformations=[]
    s = SparkPipelineState(); s.source_exists=True; s.spark_initialized=True
    s.schema_inferred=True; s.output_written=True; s.output_format="delta"; s.dq_passed=True
    r = GoalComparator().compare(goal, s)
    assert r.is_complete
def test_tool_validator():
    from tools.spark_tools import TOOL_REGISTRY
    v = ToolValidator(); s = SparkPipelineState()
    ok, _ = v.validate("initialize_spark",{"master":"local[*]","app_name":"test"},s,TOOL_REGISTRY)
    assert ok
