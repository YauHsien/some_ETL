import context
import wic.etl
from wic.etl import stage

def test_stage_order():
    assert stage.db_schema.update < stage.cm.update
    assert stage.cm.ctl_obj.update < stage.cm.update

def test_stages_sort():
    stages = [stage.db_schema.update,
              stage.cm.update,
              stage.cm.check,
              stage.cm.ctl_obj.update,
              stage.cm.ctl_obj.check,
              stage.fm.update,
              stage.pm.update,
              stage.pm.daily,
              stage.pm.hourly]
    #print(sorted(stages))
    #assert sorted(stages) == sorted(stages)
    #assert sorted(stages) == stages
    assert sorted(stages) == stage.sequence
