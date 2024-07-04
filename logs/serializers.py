from rest_framework import serializers
from .models import Log
from bson import ObjectId

class LogSerializer(serializers.ModelSerializer):
    class Meta:
        model = Log
        fields = '__all__'

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        if '_id' in ret and isinstance(ret['_id'], ObjectId):
            ret['_id'] = str(ret['_id'])
        return ret
