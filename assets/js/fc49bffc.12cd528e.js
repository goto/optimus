"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[9643],{3905:(t,e,a)=>{a.d(e,{Zo:()=>m,kt:()=>k});var n=a(7294);function r(t,e,a){return e in t?Object.defineProperty(t,e,{value:a,enumerable:!0,configurable:!0,writable:!0}):t[e]=a,t}function l(t,e){var a=Object.keys(t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(t);e&&(n=n.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),a.push.apply(a,n)}return a}function o(t){for(var e=1;e<arguments.length;e++){var a=null!=arguments[e]?arguments[e]:{};e%2?l(Object(a),!0).forEach((function(e){r(t,e,a[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(a,e))}))}return t}function p(t,e){if(null==t)return{};var a,n,r=function(t,e){if(null==t)return{};var a,n,r={},l=Object.keys(t);for(n=0;n<l.length;n++)a=l[n],e.indexOf(a)>=0||(r[a]=t[a]);return r}(t,e);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(t);for(n=0;n<l.length;n++)a=l[n],e.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(t,a)&&(r[a]=t[a])}return r}var i=n.createContext({}),u=function(t){var e=n.useContext(i),a=e;return t&&(a="function"==typeof t?t(e):o(o({},e),t)),a},m=function(t){var e=u(t.components);return n.createElement(i.Provider,{value:e},t.children)},s="mdxType",c={inlineCode:"code",wrapper:function(t){var e=t.children;return n.createElement(n.Fragment,{},e)}},d=n.forwardRef((function(t,e){var a=t.components,r=t.mdxType,l=t.originalType,i=t.parentName,m=p(t,["components","mdxType","originalType","parentName"]),s=u(a),d=r,k=s["".concat(i,".").concat(d)]||s[d]||c[d]||l;return a?n.createElement(k,o(o({ref:e},m),{},{components:a})):n.createElement(k,o({ref:e},m))}));function k(t,e){var a=arguments,r=e&&e.mdxType;if("string"==typeof t||r){var l=a.length,o=new Array(l);o[0]=d;var p={};for(var i in e)hasOwnProperty.call(e,i)&&(p[i]=e[i]);p.originalType=t,p[s]="string"==typeof t?t:r,o[1]=p;for(var u=2;u<l;u++)o[u]=a[u];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}d.displayName="MDXCreateElement"},2723:(t,e,a)=>{a.r(e),a.d(e,{assets:()=>i,contentTitle:()=>o,default:()=>c,frontMatter:()=>l,metadata:()=>p,toc:()=>u});var n=a(7462),r=(a(7294),a(3905));const l={},o="Metrics",p={unversionedId:"reference/metrics",id:"reference/metrics",title:"Metrics",description:"Job Change Metrics",source:"@site/docs/reference/metrics.md",sourceDirName:"reference",slug:"/reference/metrics",permalink:"/optimus/docs/reference/metrics",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/reference/metrics.md",tags:[],version:"current",lastUpdatedBy:"Oky Setiawan",lastUpdatedAt:1719393045,formattedLastUpdatedAt:"Jun 26, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"API",permalink:"/optimus/docs/reference/api"},next:{title:"FAQ",permalink:"/optimus/docs/reference/faq"}},i={},u=[{value:"Job Change Metrics",id:"job-change-metrics",level:2},{value:"JobRun Metrics",id:"jobrun-metrics",level:2},{value:"Resource Metrics",id:"resource-metrics",level:2},{value:"Tenant Metrics",id:"tenant-metrics",level:2},{value:"System Metrics",id:"system-metrics",level:2}],m={toc:u},s="wrapper";function c(t){let{components:e,...a}=t;return(0,r.kt)(s,(0,n.Z)({},m,a,{components:e,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"metrics"},"Metrics"),(0,r.kt)("h2",{id:"job-change-metrics"},"Job Change Metrics"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Name"),(0,r.kt)("th",{parentName:"tr",align:null},"Type"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"),(0,r.kt)("th",{parentName:"tr",align:null},"Labels"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"job_events_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of job changes attempt."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, status")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"job_upload_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of jobs uploaded to scheduler."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, status")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"job_removal_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of jobs removed from scheduler."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, status")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"job_namespace_migrations_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of jobs migrated from a namespace to another."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace_source, namespace_destination")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"job_replace_all_duration_seconds"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Duration of job 'replace-all' process in seconds."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"job_refresh_duration_seconds"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Duration of job 'refresh' process in seconds."),(0,r.kt)("td",{parentName:"tr",align:null},"project")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"job_validation_duration_seconds"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Duration of job 'validation' process in seconds."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace")))),(0,r.kt)("h2",{id:"jobrun-metrics"},"JobRun Metrics"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Name"),(0,r.kt)("th",{parentName:"tr",align:null},"Type"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"),(0,r.kt)("th",{parentName:"tr",align:null},"Labels"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"jobrun_events_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of jobrun events in a job broken by the status, e.g sla_miss, wait_upstream, in_progress, success, failed."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, job, status")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"jobrun_sensor_events_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of sensor run events broken by the event_type, e.g start, retry, success, fail."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, event_type")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"jobrun_task_events_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of task run events for a given operator (task name) broken by the event_type, e.g start, retry, success, fail."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, event_type, operator")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"jobrun_hook_events_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of hook run events for a given operator (task name) broken by the event_type, e.g start, retry, success, fail."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, event_type, operator")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"jobrun_replay_requests_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of replay requests for a single job."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, job, status")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"jobrun_alerts_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of the alerts triggered broken by the alert type."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, type")))),(0,r.kt)("h2",{id:"resource-metrics"},"Resource Metrics"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Name"),(0,r.kt)("th",{parentName:"tr",align:null},"Type"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"),(0,r.kt)("th",{parentName:"tr",align:null},"Labels"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"resource_events_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of resource change attempts broken down by the resource type and status."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, datastore, type, status")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"resource_namespace_migrations_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of resources migrated from a namespace to another namespace."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace_source, namespace_destination")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"resource_upload_all_duration_seconds"),(0,r.kt)("td",{parentName:"tr",align:null},"gauge"),(0,r.kt)("td",{parentName:"tr",align:null},"Duration of uploading all resource specification in seconds."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"resource_backup_requests_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of backup requests for a single resource."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, resource, status")))),(0,r.kt)("h2",{id:"tenant-metrics"},"Tenant Metrics"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Name"),(0,r.kt)("th",{parentName:"tr",align:null},"Type"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"),(0,r.kt)("th",{parentName:"tr",align:null},"Labels"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"secret_events_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of secret change attempts."),(0,r.kt)("td",{parentName:"tr",align:null},"project, namespace, status")))),(0,r.kt)("h2",{id:"system-metrics"},"System Metrics"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Name"),(0,r.kt)("th",{parentName:"tr",align:null},"Type"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"),(0,r.kt)("th",{parentName:"tr",align:null},"Labels"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"application_heartbeat"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Optimus server heartbeat pings."),(0,r.kt)("td",{parentName:"tr",align:null},"-")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"application_uptime_seconds"),(0,r.kt)("td",{parentName:"tr",align:null},"gauge"),(0,r.kt)("td",{parentName:"tr",align:null},"Seconds since the application started."),(0,r.kt)("td",{parentName:"tr",align:null},"-")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"notification_queue_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of items queued in the notification channel."),(0,r.kt)("td",{parentName:"tr",align:null},"type")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"notification_worker_batch_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of worker executions in the notification channel."),(0,r.kt)("td",{parentName:"tr",align:null},"type")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"notification_worker_send_err_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of events created and to be sent to writer."),(0,r.kt)("td",{parentName:"tr",align:null},"type")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"publisher_kafka_events_queued_total"),(0,r.kt)("td",{parentName:"tr",align:null},"counter"),(0,r.kt)("td",{parentName:"tr",align:null},"Number of events queued to be published to kafka topic."),(0,r.kt)("td",{parentName:"tr",align:null},"-")))))}c.isMDXComponent=!0}}]);