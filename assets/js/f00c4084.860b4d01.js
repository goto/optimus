"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[3921],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>b});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function u(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var l=n.createContext({}),c=function(e){var t=n.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},p=function(e){var t=c(e.components);return n.createElement(l.Provider,{value:t},e.children)},s="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,l=e.parentName,p=u(e,["components","mdxType","originalType","parentName"]),s=c(r),m=a,b=s["".concat(l,".").concat(m)]||s[m]||d[m]||i;return r?n.createElement(b,o(o({ref:t},p),{},{components:r})):n.createElement(b,o({ref:t},p))}));function b(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=m;var u={};for(var l in t)hasOwnProperty.call(t,l)&&(u[l]=t[l]);u.originalType=e,u[s]="string"==typeof e?e:a,o[1]=u;for(var c=2;c<i;c++)o[c]=r[c];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},4825:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>u,toc:()=>c});var n=r(7462),a=(r(7294),r(3905));const i={},o="Backup BigQuery Resource",u={unversionedId:"client-guide/backup-bigquery-resource",id:"client-guide/backup-bigquery-resource",title:"Backup BigQuery Resource",description:"Backup is a common prerequisite step to be done before re-running or modifying a resource. Currently, Optimus supports",source:"@site/docs/client-guide/backup-bigquery-resource.md",sourceDirName:"client-guide",slug:"/client-guide/backup-bigquery-resource",permalink:"/optimus/docs/client-guide/backup-bigquery-resource",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/backup-bigquery-resource.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1717647844,formattedLastUpdatedAt:"Jun 6, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Organizing Specifications",permalink:"/optimus/docs/client-guide/organizing-specifications"},next:{title:"Replay a Job (Backfill)",permalink:"/optimus/docs/client-guide/replay-a-job"}},l={},c=[{value:"Configuring backup details",id:"configuring-backup-details",level:2},{value:"Run a backup",id:"run-a-backup",level:2},{value:"Get the list of backups",id:"get-the-list-of-backups",level:2}],p={toc:c},s="wrapper";function d(e){let{components:t,...r}=e;return(0,a.kt)(s,(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"backup-bigquery-resource"},"Backup BigQuery Resource"),(0,a.kt)("p",null,"Backup is a common prerequisite step to be done before re-running or modifying a resource. Currently, Optimus supports\nbackup for BigQuery tables and provides dependency resolution, so backup can be also done to all the downstream tables\nas long as it is registered in Optimus and within the same project."),(0,a.kt)("h2",{id:"configuring-backup-details"},"Configuring backup details"),(0,a.kt)("p",null,"Several configurations can be set to have the backup result in your project as your preference. Here are the available\nconfigurations for BigQuery datastore."),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Configuration Key"),(0,a.kt)("th",{parentName:"tr",align:null},"Description"),(0,a.kt)("th",{parentName:"tr",align:null},"Default"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"ttl"),(0,a.kt)("td",{parentName:"tr",align:null},"Time to live in duration"),(0,a.kt)("td",{parentName:"tr",align:null},"720h")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"prefix"),(0,a.kt)("td",{parentName:"tr",align:null},"Prefix of the result table name"),(0,a.kt)("td",{parentName:"tr",align:null},"backup")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"dataset"),(0,a.kt)("td",{parentName:"tr",align:null},"Where the table result should be located"),(0,a.kt)("td",{parentName:"tr",align:null},"optimus_backup")))),(0,a.kt)("p",null,(0,a.kt)("em",{parentName:"p"},"Note: these values can be set in the project configuration.")),(0,a.kt)("h2",{id:"run-a-backup"},"Run a backup"),(0,a.kt)("p",null,"To start a backup, run the following command:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},'$ optimus backup create --resource "resource_name" --project sample-project --namespace sample-namespace\n')),(0,a.kt)("p",null,"After you run the command, prompts will be shown. You will need to answer the questions."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},'$ optimus backup create --resource "resource_name" --project sample-project --namespace sample-namespace\n? Select supported datastore? bigquery\n? Why is this backup needed? backfill due to business logic change\n')),(0,a.kt)("p",null,"Once the backup is finished, the backup results along with where it is located will be shown."),(0,a.kt)("h2",{id:"get-the-list-of-backups"},"Get the list of backups"),(0,a.kt)("p",null,"List of recent backups of a project can be checked using this subcommand:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus backup list --project sample-project\n")),(0,a.kt)("p",null,"Recent backup ID including the resource, when it was created, what is the description or purpose of the backup will\nbe shown. The backup ID is used as a postfix in the backup result name, thus you can find those results in the datastore\n(for example BigQuery) using the backup ID. However, keep in mind that these backup results have an expiry time set."))}d.isMDXComponent=!0}}]);