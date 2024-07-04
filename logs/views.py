from django.shortcuts import render
from rest_framework import viewsets, filters
from .models import Log
from .serializers import LogSerializer
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework import status
from djongo.database import connect
from rest_framework.pagination import PageNumberPagination
from kafka import KafkaProducer
import json
from bson import ObjectId

class LogPagination(PageNumberPagination):
    page_size = 5
    page_size_query_param = 'page_size'
    max_page_size = 100

class LogViewSet(viewsets.ModelViewSet):
    queryset = Log.objects.all()
    serializer_class = LogSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['log_id', 'timestamp', 'log_type']
    search_fields = ['log_id', 'log_type', 'message']
    ordering_fields = ['timestamp']
    ordering = ['log_id'] 
    pagination_class = LogPagination

    def perform_create(self, serializer):
        
        serializer.save()
        log_data = serializer.data

        
        if '_id' in log_data:
            log_data['_id'] = str(log_data['_id'])

        
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send('logs', log_data)
        producer.flush()

        print(f"Sent log data to Kafka: {log_data}")

    

    @action(detail=False, methods=['get'])
    def search(self, request):
        name = request.query_params.get('name')
        date_gt = request.query_params.get('date_gt')
        date_lt = request.query_params.get('date_lt')
        date_eq = request.query_params.get('date_eq')
        log_id = request.query_params.get('log_id')
        timestamp = request.query_params.get('timestamp')
        log_type = request.query_params.get('log_type')

        query = {}

        if name:
            query['message__icontains'] = name
        if date_gt:
            query['timestamp__gt'] = date_gt
        if date_lt:
            query['timestamp__lt'] = date_lt
        if date_eq:
            query['timestamp'] = date_eq
        if log_id:
            query['log_id'] = log_id
        if timestamp:
            query['timestamp'] = timestamp
        if log_type:
            query['log_type'] = log_type

        logs = Log.objects.filter(**query)
        page = self.paginate_queryset(logs)
        if page is not None:
            serializer = self.get_paginated_response(LogSerializer(page, many=True).data)
        else:
            serializer = LogSerializer(logs, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
