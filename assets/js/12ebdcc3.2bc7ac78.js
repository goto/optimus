"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2106],{3905:function(e,t,n){n.d(t,{Zo:function(){return d},kt:function(){return h}});var o=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,o,r=function(e,t){if(null==e)return{};var n,o,r={},a=Object.keys(e);for(o=0;o<a.length;o++)n=a[o],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(o=0;o<a.length;o++)n=a[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var u=o.createContext({}),l=function(e){var t=o.useContext(u),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},d=function(e){var t=l(e.components);return o.createElement(u.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},p=o.forwardRef((function(e,t){var n=e.components,r=e.mdxType,a=e.originalType,u=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),p=l(n),h=r,m=p["".concat(u,".").concat(h)]||p[h]||c[h]||a;return n?o.createElement(m,i(i({ref:t},d),{},{components:n})):o.createElement(m,i({ref:t},d))}));function h(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var a=n.length,i=new Array(a);i[0]=p;var s={};for(var u in t)hasOwnProperty.call(t,u)&&(s[u]=t[u]);s.originalType=e,s.mdxType="string"==typeof e?e:r,i[1]=s;for(var l=2;l<a;l++)i[l]=n[l];return o.createElement.apply(null,i)}return o.createElement.apply(null,n)}p.displayName="MDXCreateElement"},4365:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return s},contentTitle:function(){return u},metadata:function(){return l},toc:function(){return d},default:function(){return p}});var o=n(7462),r=n(3366),a=(n(7294),n(3905)),i=["components"],s={},u=void 0,l={unversionedId:"rfcs/revisit_automated_dependency_resolution",id:"rfcs/revisit_automated_dependency_resolution",isDocsHomePage:!1,title:"revisit_automated_dependency_resolution",description:"- Feature Name: Revisit Automated Dependency Resolution Logic",source:"@site/docs/rfcs/20220124_revisit_automated_dependency_resolution.md",sourceDirName:"rfcs",slug:"/rfcs/revisit_automated_dependency_resolution",permalink:"/optimus/docs/rfcs/revisit_automated_dependency_resolution",editUrl:"https://github.com/odpf/optimus/edit/master/docs/docs/rfcs/20220124_revisit_automated_dependency_resolution.md",tags:[],version:"current",lastUpdatedBy:"Dery Rahman Ahaddienata",lastUpdatedAt:1673927303,formattedLastUpdatedAt:"1/17/2023",sidebarPosition:20220124,frontMatter:{}},d=[{value:"Background :",id:"background-",children:[]},{value:"Approach :",id:"approach-",children:[]},{value:"Other Thoughts:",id:"other-thoughts",children:[]},{value:"How do we transition to this new approach?",id:"how-do-we-transition-to-this-new-approach",children:[]}],c={toc:d};function p(e){var t=e.components,n=(0,r.Z)(e,i);return(0,a.kt)("wrapper",(0,o.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Feature Name: Revisit Automated Dependency Resolution Logic"),(0,a.kt)("li",{parentName:"ul"},"Status: Draft"),(0,a.kt)("li",{parentName:"ul"},"Start Date: 2022-01-24"),(0,a.kt)("li",{parentName:"ul"},"Authors: ")),(0,a.kt)("h1",{id:"summary"},"Summary"),(0,a.kt)("p",null,"Optimus is a data warehouse management system, data is at the core of Optimus. Automated dependency resolution is the core problem which Optimus wants to address. The current windowing is confusing, so lets take a revisit to understand how we can solve this problem if we are tackling it fresh."),(0,a.kt)("h1",{id:"technical-design"},"Technical Design"),(0,a.kt)("h3",{id:"background-"},"Background :"),(0,a.kt)("p",null,"Input Data Flows through the system & it is expected to arrive at some delay or user gives enough buffer for all late data to arrive. Post that, the user expects to schedule the job to process the data after the max delay."),(0,a.kt)("p",null,"Keeping this basic idea in mind, what logic can be used to enable automated dependency resolution is the key question for us? And what all questions need to be answered for the same?"),(0,a.kt)("p",null,"Question 1 : What is the time range of data a job consumes from the primary sources?"),(0,a.kt)("p",null,"Question 2 : What is the time range of data a job writes?"),(0,a.kt)("p",null,"If these two questions be answered for every scheduled job then dependent jobs be computed accordingly."),(0,a.kt)("h3",{id:"approach-"},"Approach :"),(0,a.kt)("p",null,"Let's answer the ",(0,a.kt)("strong",{parentName:"p"},"Question 1"),", this is clearly a user input, there is no computation here. How intuitively a user input can be taken is the key here."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"data_window : \n max_delay : 1d/2h/1d2h\n amount : 1d/1w/2d/1m \n")),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"max_delay")," is used to identify the end time of the window."),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"amount")," is the amount of data the user is consuming from the primary sources."),(0,a.kt)("p",null,"Below is the current windowing configuration."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"  window:\n    size: 24h\n    offset: 24h\n    truncate_to: d\n")),(0,a.kt)("p",null,"Let's answer ",(0,a.kt)("strong",{parentName:"p"},"Question 2"),", I believe this is mainly linked with the schedule of the job, if the job is scheduled daily then the expectation is the job makes data available for a whole day, if hourly then for a whole hour, irrespective of the input_window. What exactly is the time range can be computed by ",(0,a.kt)("inlineCode",{parentName:"p"},"max_delay")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"schedule_frequency")," . "),(0,a.kt)("p",null,"If the job has a max_delay of 2h & the job is scheduled at 2 AM UTC then the job is making the data available for the entire previous day, irrespective of the window(1d,1m,1w)."),(0,a.kt)("p",null,"WIth this core idea there are few scenarios which should not be allowed or which cannot be used for automated depedency resolution to work in those cases the jobs just depend on the previous jobs for eg., dynamic schedules. If a job is scheduled only 1st,2nd & 3rd hour of the day."),(0,a.kt)("p",null,"The next part of the solution is how to do the actual dependency resolution."),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},"Compute the data_window based on user input & the schedule time."),(0,a.kt)("li",{parentName:"ol"},"Identify all the upstreams."),(0,a.kt)("li",{parentName:"ol"},"Starting from the current schedule_time get each upstream's schedule in the past & future and compute the output data range till it finds runs which falls outside the window in each direction.")),(0,a.kt)("h3",{id:"other-thoughts"},"Other Thoughts:"),(0,a.kt)("p",null,"Inorder to keep things simple for most of the users, if a user doesn't define any window then the jobs depend on the immediate upstream by schedule_time for that job & as well as the jobs that are depending the above job."),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"dstart")," & ",(0,a.kt)("inlineCode",{parentName:"p"},"dend")," macros will still be offered to users which they can use for data filtering in their queries."),(0,a.kt)("h3",{id:"how-do-we-transition-to-this-new-approach"},"How do we transition to this new approach?"),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},"Support Old & New approaches both, migrate all jobs to the new strategy & later cleanup the code.")))}p.isMDXComponent=!0}}]);