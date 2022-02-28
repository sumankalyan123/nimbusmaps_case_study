"""nimbusappapi URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from decisionApi import views

urlpatterns = [
    path('',views.home,name='home'),
    path('admin/', admin.site.urls),
    path('get_data/',views.get_data,name='get_data'),
    path('show_design_diagram/',views.show_design_diagram,name='show_design_diagram'),
    path('show_dataflow/',views.show_dataflow,name='show_dataflow'),
    path('show_count/',views.get_record_count_snowflake,name='get_record_count_snowflake')
]
