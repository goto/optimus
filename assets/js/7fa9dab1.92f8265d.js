"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[9047],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>h});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var d=a.createContext({}),s=function(e){var t=a.useContext(d),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=s(e.components);return a.createElement(d.Provider,{value:t},e.children)},u="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},c=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,d=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=s(n),c=r,h=u["".concat(d,".").concat(c)]||u[c]||m[c]||o;return n?a.createElement(h,i(i({ref:t},p),{},{components:n})):a.createElement(h,i({ref:t},p))}));function h(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=c;var l={};for(var d in t)hasOwnProperty.call(t,d)&&(l[d]=t[d]);l.originalType=e,l[u]="string"==typeof e?e:r,i[1]=l;for(var s=2;s<o;s++)i[s]=n[s];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}c.displayName="MDXCreateElement"},8068:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>d,contentTitle:()=>i,default:()=>m,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var a=n(7462),r=(n(7294),n(3905));const o={},i="Intervals and Windows",l={unversionedId:"concepts/intervals-and-windows",id:"concepts/intervals-and-windows",title:"Intervals and Windows",description:"When defining a new job, you need to define the interval (cron) at which it will be triggered. This parameter can give",source:"@site/docs/concepts/intervals-and-windows.md",sourceDirName:"concepts",slug:"/concepts/intervals-and-windows",permalink:"/optimus/docs/concepts/intervals-and-windows",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/intervals-and-windows.md",tags:[],version:"current",lastUpdatedBy:"Dery Rahman Ahaddienata",lastUpdatedAt:1692593449,formattedLastUpdatedAt:"Aug 21, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Macros",permalink:"/optimus/docs/concepts/macros"},next:{title:"Secret",permalink:"/optimus/docs/concepts/secret"}},d={},s=[{value:"Window Configuration",id:"window-configuration",level:2}],p={toc:s},u="wrapper";function m(e){let{components:t,...n}=e;return(0,r.kt)(u,(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"intervals-and-windows"},"Intervals and Windows"),(0,r.kt)("p",null,"When defining a new job, you need to define the ",(0,r.kt)("strong",{parentName:"p"},"interval (cron)")," at which it will be triggered. This parameter can give\nyou a precise value when the job is scheduled for execution but only a rough estimate exactly when the job is executing.\nIt is very common in a ETL pipeline to know when the job is exactly executing as well as for what time window the current\ntransformation will consume the data."),(0,r.kt)("p",null,"For example, assume there is a job that querying from a table using below statement:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"SELECT * FROM table WHERE\ncreated_at >= DATE('{{.DSTART}}') AND\ncreated_at < DATE('{{.DEND}}')\n")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},(0,r.kt)("em",{parentName:"strong"},"DSTART"))," and ",(0,r.kt)("strong",{parentName:"p"},(0,r.kt)("em",{parentName:"strong"},"DEND"))," could be replaced at the time of compilation with based on its window configuration.\nWithout the provided filter, we will have to consume all the records which are created till date inside the table\neven though the previous rows might already been processed."),(0,r.kt)("p",null,"These ",(0,r.kt)("em",{parentName:"p"},"DSTART")," and ",(0,r.kt)("em",{parentName:"p"},"DEND")," values of the input window could vary depending on the ETL job requirement."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"For a simple transformation job executing daily, it would need to consume full day work of yesterday\u2019s data."),(0,r.kt)("li",{parentName:"ul"},"A job might be consuming data for a week/month for an aggregation job, but the data boundaries should be complete,\nnot consuming any partial data of a day.")),(0,r.kt)("h2",{id:"window-configuration"},"Window Configuration"),(0,r.kt)("p",null,"Optimus allows user to define the amount of data window to consume through window configurations. The configurations\nact on the schedule",(0,r.kt)("em",{parentName:"p"},"time of the job and applied in order to compute _DSTART")," and ",(0,r.kt)("em",{parentName:"p"},"DEND"),"."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"Truncate_to"),': The data window on most of the scenarios needs to be aligned to a well-defined time window\nlike month start to month end, or week start to weekend with week start being monday, or a complete day.\nInorder to achieve that the truncate_to option is provided which can be configured with either of these values\n"h", "d", "w", "M" through which for a given schedule_time the end_time will be the end of last hour, day, week, month respectively.'),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"Offset"),": Offset is time duration configuration which enables user to move the ",(0,r.kt)("inlineCode",{parentName:"li"},"end_time"),' post truncation.\nUser can define the duration like "24h", "2h45m", "60s", "-45m24h", "0", "", "2M", "45M24h", "45M24h30m"\nwhere "h","m","s","M" means hour, month, seconds, Month respectively.'),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"Size"),": Size enables user to define the amount of data to consume from the ",(0,r.kt)("inlineCode",{parentName:"li"},"end_time")," again defined through the duration same as offset.")),(0,r.kt)("p",null,"For example, previous-mentioned job has ",(0,r.kt)("inlineCode",{parentName:"p"},"0 2 * * *")," schedule interval and is scheduled to run on\n",(0,r.kt)("strong",{parentName:"p"},"2023-03-07 at 02.00 UTC")," with following details:"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Configuration"),(0,r.kt)("th",{parentName:"tr",align:null},"Value"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Truncate_to"),(0,r.kt)("td",{parentName:"tr",align:null},"d"),(0,r.kt)("td",{parentName:"tr",align:null},"Even though it is scheduled at 02.00 AM, data window will be day-truncated (00.00 AM).")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Offset"),(0,r.kt)("td",{parentName:"tr",align:null},"-24h"),(0,r.kt)("td",{parentName:"tr",align:null},"Shifts the window to be 1 day earlier.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Size"),(0,r.kt)("td",{parentName:"tr",align:null},"24h"),(0,r.kt)("td",{parentName:"tr",align:null},"Gap between DSTART and DEND is 24h.")))),(0,r.kt)("p",null,"Above configuration will produce below window:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("em",{parentName:"li"},"DSTART"),": 2023-04-05T00:00:00Z"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("em",{parentName:"li"},"DEND"),": 2023-04-06T00:00:00Z")),(0,r.kt)("p",null,"This means, the query will be compiled to the following query"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"SELECT * FROM table WHERE\ncreated_at >= DATE('2023-04-05T00:00:00Z') AND\ncreated_at < DATE('2023-04-06T00:00:00Z')\n")),(0,r.kt)("p",null,"Assume the table content is as the following:"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"name"),(0,r.kt)("th",{parentName:"tr",align:null},"created_at"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Rick"),(0,r.kt)("td",{parentName:"tr",align:null},"2023-03-05")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Sanchez"),(0,r.kt)("td",{parentName:"tr",align:null},"2023-03-06")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Serious"),(0,r.kt)("td",{parentName:"tr",align:null},"2023-03-07")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Sam"),(0,r.kt)("td",{parentName:"tr",align:null},"2023-03-07")))),(0,r.kt)("p",null,"When the job that scheduled at ",(0,r.kt)("strong",{parentName:"p"},"2023-03-07")," runs, the job will consume ",(0,r.kt)("inlineCode",{parentName:"p"},"Rick")," as the input of the table."),(0,r.kt)("p",null,"The above expectation of windowing is properly handled in job spec version 2, version 1 has some limitations in some of\nthese expectations. You can verify these configurations by trying out in below command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"$ optimus playground\n")))}m.isMDXComponent=!0}}]);