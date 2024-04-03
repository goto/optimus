"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[7139],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>m});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var c=n.createContext({}),l=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},p=function(e){var t=l(e.components);return n.createElement(c.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},f=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,c=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=l(r),f=a,m=u["".concat(c,".").concat(f)]||u[f]||d[f]||o;return r?n.createElement(m,i(i({ref:t},p),{},{components:r})):n.createElement(m,i({ref:t},p))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=f;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s[u]="string"==typeof e?e:a,i[1]=s;for(var l=2;l<o;l++)i[l]=r[l];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}f.displayName="MDXCreateElement"},3001:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>c,contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>s,toc:()=>l});var n=r(7462),a=(r(7294),r(3905));const o={},i="FAQ",s={unversionedId:"reference/faq",id:"reference/faq",title:"FAQ",description:"- I want to run a DDL/DML query like DELETE, how can I do that?",source:"@site/docs/reference/faq.md",sourceDirName:"reference",slug:"/reference/faq",permalink:"/optimus/docs/reference/faq",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/reference/faq.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1712122508,formattedLastUpdatedAt:"Apr 3, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Metrics",permalink:"/optimus/docs/reference/metrics"}},c={},l=[],p={toc:l},u="wrapper";function d(e){let{components:t,...r}=e;return(0,a.kt)(u,(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"faq"},"FAQ"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},(0,a.kt)("strong",{parentName:"p"},"I want to run a DDL/DML query like DELETE, how can I do that?")),(0,a.kt)("p",{parentName:"li"},"Write SQL query as you would write in BQ UI and select the load method as MERGE.\nThis will execute even if the query does not contain a merge statement.")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},(0,a.kt)("strong",{parentName:"p"},"What should not be changed once the specifications are created?")),(0,a.kt)("p",{parentName:"li"},"Optimus uses Airflow for scheduling job execution and it does not support change of start_date & schedule_interval\nonce the dag is created. For that please delete the existing one or recreate it with a different suffix.\nAlso make sure you don\u2019t change the dag name if you don\u2019t want to lose the run history of Airflow.")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},(0,a.kt)("strong",{parentName:"p"},"Can I have a job with only a transformation task (without a hook) or the other way around?")),(0,a.kt)("p",{parentName:"li"},"Transformation task is mandatory but hook is optional. You can have a transformation task without a hook but cannot\nhave a hook without a transformation task.")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},(0,a.kt)("strong",{parentName:"p"},"I have a job with a modified view source, however, it does not detect as modified when running the job replace-all command.\nHow should I apply the change?")),(0,a.kt)("p",{parentName:"li"},"It does not detect as modified as the specification and the assets of the job itself is not changed. Do run the job refresh command.")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},(0,a.kt)("strong",{parentName:"p"},"My job is failing due to the resource is not sufficient. Can I scale a specific job to have a bigger CPU / memory limit?")),(0,a.kt)("p",{parentName:"li"},"Yes, resource requests and limits are configurable in the job specification\u2019s metadata field.")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},(0,a.kt)("strong",{parentName:"p"},"I removed a resource specification, but why does the resource is not deleted in the BigQuery project?")),(0,a.kt)("p",{parentName:"li"},"Optimus currently does not support resource deletion to avoid any accidental deletion.")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},(0,a.kt)("strong",{parentName:"p"},"I removed a job specification, but why does it still appear in Airflow UI?")),(0,a.kt)("p",{parentName:"li"},"You might want to check the log of the replace-all command you are using. It will show whether your job has been\nsuccessfully deleted or not. If not, the possible causes are the job is being used by another job as a dependency.\nYou can force delete it through API if needed."),(0,a.kt)("p",{parentName:"li"},"If the job has been successfully deleted, it might take time for the deletion to reflect which depends on your Airflow sync or DAG load configuration."))))}d.isMDXComponent=!0}}]);